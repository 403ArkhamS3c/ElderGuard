import os
import csv
import json
import time
import math
import uuid
import asyncio
import logging
from collections import deque
from typing import Literal, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

try:
    from supabase import create_client, Client
except ImportError:  # API tetap dapat berjalan secara lokal tanpa paket supabase
    create_client = None
    Client = object

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("elderguard")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# Konfigurasi Supabase (fallback ke nilai environment atau default proyek jika tersedia)
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://btakpzeymfjuwnlfzbls.supabase.co").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
supabase = None
if SUPABASE_URL and SUPABASE_KEY and create_client is not None:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as exc:
        log.warning("Gagal menginisialisasi Supabase (%s); berjalan dalam mode CSV lokal.", exc)
else:
    log.warning("Supabase belum dikonfigurasi; data hanya disimpan ke CSV/log lokal.")

# Satuan tekanan dari sensor: Android TYPE_PRESSURE umumnya mengirimkan hPa/mbar.
PRESSURE_UNIT = os.getenv("PRESSURE_UNIT", "hPa").strip().lower()
if PRESSURE_UNIT not in {"hpa", "pa"}:
    raise RuntimeError("PRESSURE_UNIT harus 'hPa' atau 'Pa'")

app = FastAPI(title="ElderGuard API v3 - Final MVP Blueprint Aligned")

# =====================================================================
# INITIAL TECHNICAL PARAMETERS (ELDERGUARD FINAL MVP BLUEPRINT)
# Catatan: Seluruh angka di bawah ini merupakan parameter engineering
# awal untuk validasi eksperimental, bukan standar klinis mutlak.
# =====================================================================
SAMPLE_RATE_HZ = 25                                                  # 25 Hz (~40 ms per sampel)

# Jendela Waktu sesuai waktu sampling 25 Hz:
PRE_CONTEXT_SECONDS = 3.5                                            # Pre-event context: ~3–3.5 s
IMPACT_WINDOW_SECONDS = 1.0                                          # Impact/Event window: ~1 s
RECOVERY_OBS_SECONDS = 3.5                                           # Post-event / Recovery observation: ~3–3.5 s

PRE_CONTEXT_SAMPLES = round(SAMPLE_RATE_HZ * PRE_CONTEXT_SECONDS)    # 88 sampel sebelum trigger
IMPACT_WINDOW_SAMPLES = round(SAMPLE_RATE_HZ * IMPACT_WINDOW_SECONDS)# 25 sampel saat fase impact
RECOVERY_OBS_SAMPLES = round(SAMPLE_RATE_HZ * RECOVERY_OBS_SECONDS)  # 88 sampel murni pasca-impact
TOTAL_POST_TRIGGER_SAMPLES = IMPACT_WINDOW_SAMPLES + RECOVERY_OBS_SAMPLES  # Total sampel setelah trigger

# Ambang Batas Sensor (Experimental Parameters):
ACC_TRIGGER_G = 2.5                  # Pemicu kandidat awal (ACC >= 2.0 G)
FALL_IMPACT_G = 4.5                  # Bukti dampak kuat (Strong ACC reference >= 2.5 G)
PRESSURE_THRESHOLD_PA = 10.0         # Referensi awal perubahan tekanan barometrik (ΔP ≈ 10 Pa)
PRESSURE_TOLERANCE_PA = 5.0          # Toleransi bawah perubahan tekanan untuk evaluasi data uji (ΔP >= 5 Pa)

# Filter Durasi & Repetisi:
ACTIVE_G_LEVEL = 1.3                 # Ambang gerak aktif untuk perhitungan durasi
LONG_MOVEMENT_DURATION_MS = 3000     # Duration filter: ~3 detik (3000 ms)
HIGH_G_LEVEL = 1.5                   # Ambang lonjakan berulang untuk filter repetisi
REPETITIVE_RATIO = 0.4               # Repetition filter ratio

# Indikator Temporal & Kestabilan Pasca-Kejadian:
POST_IMPACT_STABLE_VAR_G2 = 0.18     # Batas variansi (G^2) pada jendela pasca-impact (stabil/tenang)
PRE_IMPACT_LOW_G = 0.65              # Indikasi eksperimental fase unloading sebelum benturan
PRE_IMPACT_LOW_G_MIN_SAMPLES = 2     # Minimal 2 sampel berturut-turut (~80 ms pada 25 Hz)

# Operasional & Cooldown:
COOLDOWN_SEC = 2.0                   # Cooldown ~2 detik sesuai blueprint
POST_EVENT_TIMEOUT_SEC = float(os.getenv("POST_EVENT_TIMEOUT_SEC", "15"))
if POST_EVENT_TIMEOUT_SEC <= 0:
    raise RuntimeError("POST_EVENT_TIMEOUT_SEC harus lebih besar dari 0")

EVENT_LOG_FILE = "event_detection_log.csv"

sensor_buffers: dict[str, deque] = {}
active_captures: dict[str, dict] = {}
last_event_time: dict[str, float] = {}
pending_events: dict[str, dict] = {}

EVENT_FIELDS = [
    "event_id",
    "timestamp_ms",
    "acc_peak_g",
    "delta_p_pa",
    "pressure_tier",
    "duration_ms",
    "candidate_type",
    "event_type",
    "classification_reason",
    "recovery_status",
    "user_response",
    "alert_status",
    "caregiver_acknowledgement",
    "detection_latency_ms",
    "post_impact_variance_g2",
    "repetitive_motion_suspected",
    "pre_impact_unloading",
    "pressure_support",
    "post_impact_stable",
    "fall_support_count",
]


def init_csv_logs() -> None:
    if not os.path.exists(EVENT_LOG_FILE):
        with open(EVENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(EVENT_FIELDS)
        return

    try:
        with open(EVENT_LOG_FILE, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            old_fields = reader.fieldnames or []
            rows = list(reader)
        if old_fields != EVENT_FIELDS:
            migrated = [{field: old_row.get(field, "") for field in EVENT_FIELDS} for old_row in rows]
            with open(EVENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=EVENT_FIELDS)
                writer.writeheader()
                writer.writerows(migrated)
    except OSError as exc:
        log.error("Gagal menyiapkan event log: %s", exc)


init_csv_logs()


def append_csv(path: str, row: list) -> None:
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)
    except OSError as exc:
        log.error("Gagal menulis %s: %s", path, exc)


def update_event_csv(
    event_id: str,
    final_event_type: str,
    recovery_status: str,
    user_response: str,
    alert_status: str,
) -> None:
    """Memperbarui baris kandidat Fall di CSV setelah respons/timeout diterima."""
    try:
        with open(EVENT_LOG_FILE, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        found = False
        for row in rows:
            if row.get("event_id") == event_id:
                row["event_type"] = final_event_type
                row["recovery_status"] = recovery_status
                row["user_response"] = user_response
                row["alert_status"] = alert_status
                found = True
                break
        if found:
            with open(EVENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=EVENT_FIELDS)
                writer.writeheader()
                writer.writerows(rows)
        else:
            log.warning("Event %s tidak ditemukan di event log lokal", event_id)
    except OSError as exc:
        log.error("Gagal memperbarui event log: %s", exc)


def calculate_g(ax: float, ay: float, az: float) -> float:
    return math.sqrt(ax * ax + ay * ay + az * az) / 9.80665


def pressure_delta_pa(p_start: float, p_end: float) -> float:
    scale = 100.0 if PRESSURE_UNIT == "hpa" else 1.0
    return abs(p_end - p_start) * scale


def has_pre_impact_unloading(samples: list[dict], peak_idx: int) -> bool:
    lookback = SAMPLE_RATE_HZ  # ~1 detik (25 sampel) sebelum puncak impact
    start = max(0, peak_idx - lookback)
    prior = samples[start:peak_idx]
    run = 0
    for sample in prior:
        if sample["g"] <= PRE_IMPACT_LOW_G:
            run += 1
            if run >= PRE_IMPACT_LOW_G_MIN_SAMPLES:
                return True
        else:
            run = 0
    return False


def active_duration_containing_index_ms(
    g_values: list[float], timestamps_ms: list[int], center_idx: int
) -> int:
    if not g_values or len(g_values) != len(timestamps_ms):
        return 0
    if center_idx < 0 or center_idx >= len(g_values):
        return 0
    if g_values[center_idx] < ACTIVE_G_LEVEL:
        return 0
    left = center_idx
    right = center_idx
    while left > 0 and g_values[left - 1] >= ACTIVE_G_LEVEL:
        left -= 1
    while right + 1 < len(g_values) and g_values[right + 1] >= ACTIVE_G_LEVEL:
        right += 1
    return max(0, timestamps_ms[right] - timestamps_ms[left])


def evaluate_event_candidate(
    max_g: float,
    delta_p_pa: float,
    post_variance_g2: float,
    repetitive_motion_suspected: bool,
    duration_ms: int,
    pre_impact_unloading: bool = False,
) -> tuple[str, str, dict[str, bool | int | str]]:
    has_trigger = max_g >= ACC_TRIGGER_G
    has_strong_impact = max_g >= FALL_IMPACT_G

    # Evaluasi bertingkat perubahan tekanan barometrik:
    # - Full support   : ΔP >= 10.0 Pa (referensi blueprint)
    # - Tolerance band : 5.0 Pa <= ΔP < 10.0 Pa (toleransi eksperimen)
    # - Below tolerance: ΔP < 5.0 Pa (tidak ada perubahan ketinggian signifikan)
    pressure_full = delta_p_pa >= PRESSURE_THRESHOLD_PA
    pressure_within_tolerance = delta_p_pa >= PRESSURE_TOLERANCE_PA
    if pressure_full:
        pressure_tier = "full_support (>=10Pa)"
    elif pressure_within_tolerance:
        pressure_tier = "tolerance_band (5-10Pa)"
    else:
        pressure_tier = "below_tolerance (<5Pa)"

    stable_post_event = post_variance_g2 <= POST_IMPACT_STABLE_VAR_G2
    long_movement = duration_ms >= LONG_MOVEMENT_DURATION_MS

    # Hitung bukti pendukung (maksimal 3: Tekanan [termasuk toleransi >=5Pa], Stabilitas, Unloading)
    support_cues = []
    if pressure_full:
        support_cues.append(f"ΔP={delta_p_pa:.1f}Pa (>=10Pa)")
    elif pressure_within_tolerance:
        support_cues.append(f"ΔP={delta_p_pa:.1f}Pa (toleransi >=5Pa)")

    if stable_post_event:
        support_cues.append("kestabilan pasca-impact")
    if pre_impact_unloading:
        support_cues.append("unloading pra-impact")

    support_count = len(support_cues)

    evidence = {
        "trigger": has_trigger,
        "strong_impact": has_strong_impact,
        "pressure_support": pressure_within_tolerance,
        "pressure_tier": pressure_tier,
        "pre_impact_unloading": pre_impact_unloading,
        "stable_support": stable_post_event,
        "fall_support_count": support_count,
        "long_movement": long_movement,
        "repetitive_motion_suspected": repetitive_motion_suspected,
    }

    # 1. Filter Durasi & Repetisi -> No Event
    if long_movement or repetitive_motion_suspected:
        reasons = []
        if long_movement:
            reasons.append(f"durasi gerak aktif {duration_ms}ms >= {LONG_MOVEMENT_DURATION_MS}ms")
        if repetitive_motion_suspected:
            reasons.append("pola gerakan repetitif terdeteksi")
        return "No Event", "Filter durasi/repetisi: " + "; ".join(reasons), evidence

    if not has_trigger:
        return "No Event", f"ACC {max_g:.2f}G di bawah pemicu {ACC_TRIGGER_G:.1f}G", evidence

    # 2. FALL CANDIDATE (Perlu Konfirmasi Post-Event Check "ARE YOU OK?")
    # Syarat: Dampak kuat (ACC >= 2.5 G) DAN minimal 2 dari 3 bukti pendukung:
    # (1) Perubahan tekanan >= 5 Pa (termasuk pita toleransi 5-10 Pa),
    # (2) Kestabilan pasca-impact (PostVar <= 0.18 G^2),
    # (3) Unloading pra-impact (low-G sebelum benturan).
    if has_strong_impact and support_count >= 2:
        reason = (
            f"Fall Candidate: dampak kuat ACC={max_g:.2f}G (>=2.5G) "
            f"dengan {support_count}/3 bukti pendukung [{', '.join(support_cues)}]"
        )
        return "Fall Candidate", reason, evidence

    # 3. NEAR-FALL (Cabang Recovery / Hentakan Kuat Tanpa Penurunan Ketinggian)
    # Jika ACC >= 2.0 G (termasuk >= 2.5 G yang hanya punya 1 bukti pendukung karena ΔP < 5 Pa)
    # dan gerakan pasca-kejadian kembali stabil, klasifikasikan secara konsisten sebagai Near-Fall.
    if stable_post_event:
        if has_strong_impact:
            reason = (
                f"Near-Fall: dampak kuat ACC={max_g:.2f}G namun dukungan Fall hanya {support_count}/3 "
                f"(ΔP={delta_p_pa:.1f}Pa); kondisi pasca-event stabil"
            )
        else:
            reason = (
                f"Near-Fall: pemicu ACC={max_g:.2f}G (<2.5G, ΔP={delta_p_pa:.1f}Pa) "
                f"diikuti kestabilan pasca-event (var={post_variance_g2:.3f}G^2)"
            )
        return "Near-Fall", reason, evidence

    return (
        "No Event",
        f"Kriteria Fall tidak terpenuhi dan kestabilan recovery tidak terdeteksi (ACC={max_g:.2f}G, var={post_variance_g2:.3f}G^2)",
        evidence,
    )


async def db_insert(table: str, row: dict) -> None:
    if supabase is None:
        return
    try:
        await asyncio.to_thread(lambda: supabase.table(table).insert(row).execute())
    except Exception as exc:
        log.error("Supabase insert %s gagal: %s", table, exc)


async def db_update_event(event_id: str, updates: dict) -> None:
    if supabase is None:
        return
    try:
        await asyncio.to_thread(
            lambda: supabase.table("events").update(updates).eq("event_id", event_id).execute()
        )
    except Exception as exc:
        log.error("Supabase update %s gagal: %s", event_id, exc)


async def handle_fall_response_timeout(event_id: str) -> None:
    """Jika pengguna tidak merespons dalam batas waktu, tetapkan sebagai Fall — No Response & kirim Alert."""
    await asyncio.sleep(POST_EVENT_TIMEOUT_SEC)
    ev_info = pending_events.pop(event_id, None)
    if ev_info is None:
        return

    # Sesuai Bagian 5 Blueprint: Fall — No Response | Recovery: — | Response: No Response | Alert: Yes
    final_event_type = "Fall — No Response"
    await db_update_event(event_id, {"event_type": final_event_type})
    update_event_csv(
        event_id=event_id,
        final_event_type=final_event_type,
        recovery_status="—",
        user_response="No Response",
        alert_status="Yes",
    )
    await db_insert("alerts", {
        "event_id": event_id,
        "timestamp": int(time.time() * 1000),
        "alert_type": "EMERGENCY_ASSISTANCE",
        "status": "ACTIVE",
    })
    log.warning("[TIMEOUT] %s -> %s (Caregiver Alert Active)", event_id, final_event_type)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "storage": "supabase" if supabase is not None else "local_csv_only",
        "parameters": {
            "sample_rate_hz": SAMPLE_RATE_HZ,
            "acc_trigger_g": ACC_TRIGGER_G,
            "strong_acc_g": FALL_IMPACT_G,
            "pressure_ref_pa": PRESSURE_THRESHOLD_PA,
            "pressure_tolerance_pa": PRESSURE_TOLERANCE_PA,
            "pre_context_s": PRE_CONTEXT_SECONDS,
            "impact_window_s": IMPACT_WINDOW_SECONDS,
            "recovery_window_s": RECOVERY_OBS_SECONDS,
            "cooldown_s": COOLDOWN_SEC,
        },
    }


@app.websocket("/ws/sensor/{user_id}")
async def sensor_endpoint(websocket: WebSocket, user_id: str):
    await websocket.accept()
    buf = deque(maxlen=PRE_CONTEXT_SAMPLES + 1)
    sensor_buffers[user_id] = buf

    try:
        while True:
            try:
                raw_text = await websocket.receive_text()
            except WebSocketDisconnect:
                raise

            try:
                sensor_data = json.loads(raw_text)
                if not isinstance(sensor_data, dict):
                    continue
                if not all(k in sensor_data for k in ("ax", "ay", "az", "pressure")):
                    continue
                ax, ay, az, pressure = (float(sensor_data[k]) for k in ("ax", "ay", "az", "pressure"))
                watch_ts = int(sensor_data.get("timestamp", int(time.time() * 1000)))
            except (json.JSONDecodeError, TypeError, ValueError, OverflowError):
                continue

            if not all(math.isfinite(v) for v in (ax, ay, az, pressure)):
                continue

            received_mono_ms = time.monotonic() * 1000.0
            acc_g = calculate_g(ax, ay, az)
            sample = {
                "ax": ax,
                "ay": ay,
                "az": az,
                "g": acc_g,
                "pressure": pressure,
                "ts": watch_ts,
                "received_mono_ms": received_mono_ms,
            }
            buf.append(sample)

            # Saat pemicu ACC >= 2.0 G terdeteksi, bekukan konteks sebelum kejadian (~3.5 s / 88 sampel),
            # lalu kumpulkan sampel pasca-pemicu yang mencakup Impact Window (~1 s / 25 sampel)
            # ditambah Recovery/Post-Event Observation Window (~3.5 s / 88 sampel).
            if user_id in active_captures:
                capture = active_captures[user_id]
                capture["post_samples"].append(sample)
                if len(capture["post_samples"]) < TOTAL_POST_TRIGGER_SAMPLES:
                    continue
                trigger_sample = capture["trigger_sample"]
                window_data = capture["pre_samples"] + [trigger_sample] + capture["post_samples"]
                active_captures.pop(user_id, None)
            else:
                if acc_g < ACC_TRIGGER_G:
                    continue
                now_mono = time.monotonic()
                if now_mono - last_event_time.get(user_id, 0.0) < COOLDOWN_SEC:
                    continue
                pre_samples = list(buf)[:-1]
                active_captures[user_id] = {
                    "pre_samples": pre_samples[-PRE_CONTEXT_SAMPLES:],
                    "trigger_sample": sample,
                    "post_samples": [],
                }
                continue

            now_mono = time.monotonic()
            now_wall_ms = int(time.time() * 1000)
            g_values = [item["g"] for item in window_data]

            # Indeks sampel pemicu (trigger) dan pemisahan tegas 3 segmen waktu sesuai sampling 25 Hz:
            # 1) Pre-event segment  : sebelum trigger_idx (~3.5 s)
            # 2) Impact segment     : dari trigger_idx hingga impact_end_idx (~1.0 s / 25 sampel)
            # 3) Post-event segment : dari impact_end_idx hingga akhir window (~3.5 s / 88 sampel murni pasca-impact)
            trigger_idx = len(window_data) - TOTAL_POST_TRIGGER_SAMPLES - 1
            impact_end_idx = min(len(window_data), trigger_idx + IMPACT_WINDOW_SAMPLES)

            pre_segment = window_data[:trigger_idx] if trigger_idx > 0 else [window_data[0]]
            impact_segment = window_data[trigger_idx:impact_end_idx]
            post_recovery_segment = window_data[impact_end_idx:]
            if not post_recovery_segment:
                post_recovery_segment = window_data[-RECOVERY_OBS_SAMPLES:]

            # Cari puncak percepatan (impact peak) di dalam Impact Window (~1 detik)
            event_peak_local_idx = max(range(len(impact_segment)), key=lambda i: impact_segment[i]["g"])
            peak_idx = trigger_idx + event_peak_local_idx
            impact_peak_g = window_data[peak_idx]["g"]
            pre_impact_unloading = has_pre_impact_unloading(window_data, peak_idx)

            # Evaluasi filter gerakan repetitif sepanjang episode
            high_g_count = sum(g > HIGH_G_LEVEL for g in g_values)
            repetitive_motion_suspected = high_g_count > int(len(window_data) * REPETITIVE_RATIO)

            # Evaluasi perubahan tekanan barometrik (Pre-event vs Post-event recovery segment)
            p_start = sum(item["pressure"] for item in pre_segment) / len(pre_segment)
            p_end = sum(item["pressure"] for item in post_recovery_segment) / len(post_recovery_segment)
            delta_p = pressure_delta_pa(p_start, p_end)

            # Evaluasi kestabilan/pemulihan MURNI pada jendela pascakejadian (setelah impact selesai)
            post_g = [item["g"] for item in post_recovery_segment]
            mean_post_g = sum(post_g) / len(post_g)
            var_post_g = sum((g - mean_post_g) ** 2 for g in post_g) / len(post_g)

            # Durasi episode aktif di sekitar titik pemicu
            timestamps = [int(item["received_mono_ms"]) for item in window_data]
            duration_ms = active_duration_containing_index_ms(g_values, timestamps, trigger_idx)
            detection_latency_ms = max(
                0, int(time.monotonic() * 1000.0 - trigger_sample["received_mono_ms"])
            )

            candidate_type, classification_reason, evidence = evaluate_event_candidate(
                max_g=impact_peak_g,
                delta_p_pa=delta_p,
                post_variance_g2=var_post_g,
                repetitive_motion_suspected=repetitive_motion_suspected,
                duration_ms=duration_ms,
                pre_impact_unloading=pre_impact_unloading,
            )

            # Blueprint Bagian 5: No Event tidak disimpan ke database/CSV, kembali ke continuous monitoring
            if candidate_type == "No Event":
                last_event_time[user_id] = now_mono
                continue

            log.info(
                "[WINDOW] ACC=%.2fg | ΔP=%.2fPa (%s) | PostVar=%.3fG^2 | Durasi=%dms | CANDIDATE=%s",
                impact_peak_g,
                delta_p,
                evidence["pressure_tier"],
                var_post_g,
                duration_ms,
                candidate_type,
            )

            event_id = str(uuid.uuid4())
            last_event_time[user_id] = now_mono

            if candidate_type == "Near-Fall":
                # Sesuai Bagian 5 Blueprint:
                # Event Type: Near-Fall | Recovery: Detected | Response: — | Alert: No
                initial_event_type = "Near-Fall"
                recovery_status = "Detected"
                initial_response = "—"
                initial_alert = "No"
            else:
                # Kandidat Fall dipisahkan sebagai status sementara sambil menunggu konfirmasi "ARE YOU OK?"
                initial_event_type = "Fall Candidate — Waiting Confirmation"
                recovery_status = "Pending user confirmation"
                initial_response = "PENDING"
                initial_alert = "Pending"

            append_csv(EVENT_LOG_FILE, [
                event_id,
                now_wall_ms,
                f"{impact_peak_g:.3f}",
                f"{delta_p:.3f}",
                evidence["pressure_tier"],
                duration_ms,
                candidate_type,
                initial_event_type,
                classification_reason,
                recovery_status,
                initial_response,
                initial_alert,
                "Not implemented",
                detection_latency_ms,
                f"{var_post_g:.5f}",
                repetitive_motion_suspected,
                pre_impact_unloading,
                evidence["pressure_support"],
                evidence["stable_support"],
                evidence["fall_support_count"],
            ])

            if candidate_type == "Fall Candidate":
                pending_events[event_id] = {
                    "time_ms": now_wall_ms,
                    "acc_peak_g": impact_peak_g,
                    "delta_p_pa": delta_p,
                    "duration_ms": duration_ms,
                    "candidate_type": candidate_type,
                    "latency_ms": detection_latency_ms,
                    "post_variance_g2": var_post_g,
                }
                asyncio.create_task(handle_fall_response_timeout(event_id))

                # Sesuai Blueprint Bagian 1 & 4: Hanya kandidat Fall yang memicu POST-EVENT CHECK "ARE YOU OK?"
                try:
                    await websocket.send_text(json.dumps({
                        "type": "POST_EVENT_CHECK",
                        "event_id": event_id,
                        "classification": "Fall",
                    }))
                except Exception:
                    log.warning("Gagal mengirim POST_EVENT_CHECK ke user=%s", user_id)

            await db_insert("events", {
                "event_id": event_id,
                "user_id": user_id,
                "timestamp": now_wall_ms,
                "event_type": initial_event_type,
            })

    except WebSocketDisconnect:
        pass
    finally:
        active_captures.pop(user_id, None)
        if sensor_buffers.get(user_id) is buf:
            sensor_buffers.pop(user_id, None)


class WatchResponse(BaseModel):
    event_id: str
    response_type: Literal["OK", "NEED_HELP", "NO_RESPONSE"]
    classification: Optional[Literal["Near-Fall", "Fall"]] = None


@app.post("/api/events/confirm")
async def process_watch_response(payload: WatchResponse):
    ev_info = pending_events.pop(payload.event_id, None)
    if ev_info is None:
        return {
            "status": "ignored",
            "reason": "Event tidak ditemukan atau sudah ditutup oleh timeout/respons sebelumnya.",
            "event_id": payload.event_id,
        }

    # Pemetaan klasifikasi final sesuai Tabel Bagian 5 Blueprint:
    # - OK          -> Event: Fall — Recovered   | Recovery: Confirmed by user | Response: OK          | Alert: No
    # - NEED_HELP   -> Event: Fall — Need Help   | Recovery: —                 | Response: Need Help   | Alert: Yes
    # - NO_RESPONSE -> Event: Fall — No Response | Recovery: —                 | Response: No Response | Alert: Yes
    outcome_map = {
        "OK": {
            "event_type": "Fall — Recovered",
            "recovery": "Confirmed by user",
            "response": "OK",
            "alert": "No",
        },
        "NEED_HELP": {
            "event_type": "Fall — Need Help",
            "recovery": "—",
            "response": "Need Help",
            "alert": "Yes",
        },
        "NO_RESPONSE": {
            "event_type": "Fall — No Response",
            "recovery": "—",
            "response": "No Response",
            "alert": "Yes",
        },
    }

    outcome = outcome_map[payload.response_type]
    final_event_type = outcome["event_type"]

    await db_update_event(payload.event_id, {"event_type": final_event_type})
    update_event_csv(
        event_id=payload.event_id,
        final_event_type=final_event_type,
        recovery_status=outcome["recovery"],
        user_response=outcome["response"],
        alert_status=outcome["alert"],
    )

    if outcome["alert"] == "Yes":
        await db_insert("alerts", {
            "event_id": payload.event_id,
            "timestamp": int(time.time() * 1000),
            "alert_type": "EMERGENCY_ASSISTANCE",
            "status": "ACTIVE",
        })

    log.info("[CONFIRMED] %s -> %s (Alert=%s)", payload.event_id, final_event_type, outcome["alert"])

    return {
        "status": "success",
        "candidate_type": ev_info["candidate_type"],
        "recorded_as": final_event_type,
        "recovery_status": outcome["recovery"],
        "alert_status": outcome["alert"],
    }