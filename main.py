import os
import csv
import json
import math
import uuid
import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

try:
    from supabase import create_client, Client
except ImportError:
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

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
supabase = None
if SUPABASE_URL and SUPABASE_KEY and create_client is not None:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as exc:
        log.warning("Gagal menginisialisasi Supabase (%s); berjalan dalam mode CSV lokal.", exc)
else:
    log.warning("Supabase belum dikonfigurasi; data disimpan ke CSV/log lokal.")

app = FastAPI(title="ElderGuard Cloud API - Official Blueprint v1.0 Compliance")

# =====================================================================
# SYSTEM SPECIFICATIONS (BLUEPRINT v1.0 & IT BRIEF v1.0)
# =====================================================================
SAMPLING_RATE_HZ = 50                     # Target 50 Hz[cite: 3, 4]
POST_EVENT_WINDOW_SEC = 3                 # 3 seconds observation window[cite: 3, 4]
EXPECTED_SAMPLES = SAMPLING_RATE_HZ * POST_EVENT_WINDOW_SEC  # 150 samples[cite: 3, 4]

TRIGGER_THRESHOLD_G = 2.0                 # Dynamic acceleration trigger threshold[cite: 4]
FALL_IMPACT_THRESHOLD_G = 2.3             # Resultant acceleration fall threshold[cite: 4]
FALL_INACTIVITY_THRESHOLD_G = 0.2         # Inactivity dynamic acceleration limit[cite: 4]

EVENT_LOG_FILE = "elderguard_official_log.csv"

EVENT_FIELDS = [
    "id",
    "user_id",
    "device_id",
    "event_type",
    "event_timestamp",
    "peak_acceleration",
    "variability",
    "post_activity",
    "confirmation_status",
    "caregiver_notification_status",
    "reason",
]

def init_csv_logs() -> None:
    if not os.path.exists(EVENT_LOG_FILE):
        with open(EVENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(EVENT_FIELDS)

init_csv_logs()

def append_csv(row: list) -> None:
    try:
        with open(EVENT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)
    except OSError as exc:
        log.error("Gagal menulis CSV: %s", exc)

async def db_insert(table: str, row: dict) -> None:
    if supabase is None:
        return
    try:
        await asyncio.to_thread(lambda: supabase.table(table).insert(row).execute())
    except Exception as exc:
        log.error("Supabase insert %s gagal: %s", table, exc)


# =====================================================================
# RULE-BASED CLASSIFICATION ENGINE (3 CLASSES: NORMAL, NEAR_FALL, FALL)
# =====================================================================
def classify_event_rule_based(x_vals: List[float], y_vals: List[float], z_vals: List[float]) -> dict:
    n = min(len(x_vals), len(y_vals), len(z_vals))
    if n == 0:
        raise ValueError("Data sensor kosong")

    # 1. Resultant Acceleration Calculation (A_r = sqrt(ax^2 + ay^2 + az^2))[cite: 3, 4]
    resultant_g_vals = []
    for i in range(n):
        ax, ay, az = x_vals[i], y_vals[i], z_vals[i]
        a_ms2 = math.sqrt(ax * ax + ay * ay + az * az)
        resultant_g_vals.append(a_ms2 / 9.80665)

    peak_acceleration = max(resultant_g_vals)

    # 2. Gravity Estimation & Dynamic Acceleration Calculation[cite: 3, 4]
    gravity_est = sum(resultant_g_vals) / n
    dynamic_g_vals = [abs(val - gravity_est) for val in resultant_g_vals]
    peak_dynamic_g = max(dynamic_g_vals)

    # 3. Acceleration Variability & Post-Activity Feature Extraction[cite: 3, 4]
    mean_dynamic = sum(dynamic_g_vals) / n
    variability = sum((dg - mean_dynamic) ** 2 for dg in dynamic_g_vals) / n
    
    # Post-trigger wrist activity (aktivitas di bagian akhir observation window)[cite: 3, 4]
    late_window = dynamic_g_vals[n // 2:]
    post_activity = sum(late_window) / len(late_window) if late_window else 0.0

    # Filter Isolated Impact (misal: mengetuk meja / waving tajam)[cite: 3, 4]
    high_dynamic_count = sum(1 for dg in dynamic_g_vals if dg >= 1.2)
    is_isolated_impact = (peak_dynamic_g >= 2.0) and (high_dynamic_count < 3)

    # 4. Post-Event Inactivity Evaluation for Fall[cite: 3, 4]
    consecutive_low_dynamic = 0
    max_consecutive_low = 0
    for dg in dynamic_g_vals:
        if dg < FALL_INACTIVITY_THRESHOLD_G:
            consecutive_low_dynamic += 1
            if consecutive_low_dynamic > max_consecutive_low:
                max_consecutive_low = consecutive_low_dynamic
        else:
            consecutive_low_dynamic = 0

    inactivity_duration_achieved = max_consecutive_low / float(SAMPLING_RATE_HZ)
    is_fall = (peak_acceleration >= FALL_IMPACT_THRESHOLD_G) and (inactivity_duration_achieved >= 0.8)

    # --- CLASSIFICATION PRIORITY: FALL -> NEAR_FALL -> NORMAL[cite: 3, 4] ---
    if is_fall:
        return {
            "event_type": "FALL",
            "peak_acceleration": round(peak_acceleration, 3),
            "variability": round(variability, 4),
            "post_activity": round(post_activity, 4),
            "reason": f"Fall terdeteksi: Peak={peak_acceleration:.2f}G, Inactivity={inactivity_duration_achieved:.3f}s."
        }

    # NEAR_FALL Evaluation[cite: 3, 4]
    has_sudden_movement = peak_dynamic_g >= 1.5
    has_instability = variability >= 0.05 and peak_acceleration >= 2.2
    has_sustained_disturbance = high_dynamic_count >= 5

    if has_sudden_movement and has_instability and has_sustained_disturbance and not is_isolated_impact:
        return {
            "event_type": "NEAR_FALL",
            "peak_acceleration": round(peak_acceleration, 3),
            "variability": round(variability, 4),
            "post_activity": round(post_activity, 4),
            "reason": f"Near-Fall terdeteksi: Variabilitas={variability:.3f}, Peak={peak_acceleration:.2f}G"
        }

    # NORMAL[cite: 3, 4]
    return {
        "event_type": "NORMAL",
        "peak_acceleration": round(peak_acceleration, 3),
        "variability": round(variability, 4),
        "post_activity": round(post_activity, 4),
        "reason": "Aktivitas normal (tidak memenuhi kriteria Near-Fall atau Fall)."
    }


# =====================================================================
# REQUEST & RESPONSE SCHEMAS
# =====================================================================
class SensorObservationWindow(BaseModel):
    x: List[float]
    y: List[float]
    z: List[float]

class EventPacket(BaseModel):
    event_id: Optional[str] = None
    user_id: Optional[str] = "00000000-0000-0000-0000-000000000000"
    device_id: Optional[str] = "Samsung-Galaxy-Watch4"
    timestamp: Optional[str] = None
    observation_window: SensorObservationWindow


@app.post("/api/events/evaluate")
async def evaluate_event(packet: EventPacket):
    x = packet.observation_window.x
    y = packet.observation_window.y
    z = packet.observation_window.z

    if not (len(x) == len(y) == len(z)):
        raise HTTPException(status_code=400, detail="Panjang array sensor X, Y, dan Z tidak sinkron.")

    event_id = packet.event_id or str(uuid.uuid4())
    user_id = packet.user_id
    device_id = packet.device_id
    timestamp_str = packet.timestamp or datetime.now(timezone.utc).isoformat()

    # Jalankan klasifikasi rule-based sesuai blueprint[cite: 3, 4]
    result = classify_event_rule_based(x_vals=x, y_vals=y, z_vals=z)
    event_type = result["event_type"]

    # Pemisahan Status Sesuai Blueprint & IT Brief[cite: 3, 4]
    if event_type == "NORMAL":
        confirmation_status = "NOT_REQUIRED"
        notification_status = "NOT_REQUIRED"
    else:
        confirmation_status = "PENDING"  # Memerlukan human confirmation (30s timeout)[cite: 3, 4]
        notification_status = "PENDING"

    log.info(
        "[EVALUATE] id=%s | Type=%s | Peak=%.2fG | Var=%.4f | ConfStatus=%s | NotifStatus=%s",
        event_id,
        event_type,
        result["peak_acceleration"],
        result["variability"],
        confirmation_status,
        notification_status,
    )

    # 1. Simpan ke CSV Log Lokal[cite: 3, 4]
    append_csv([
        event_id,
        user_id,
        device_id,
        event_type,
        timestamp_str,
        result["peak_acceleration"],
        result["variability"],
        result["post_activity"],
        confirmation_status,
        notification_status,
        result["reason"],
    ])

    # 2. Sinkronisasi ke Supabase Database (`events` table schema compliance)[cite: 3, 4]
    await db_insert("events", {
        "id": event_id,
        "user_id": user_id,
        "device_id": device_id,
        "event_type": event_type,
        "event_timestamp": timestamp_str,
        "peak_acceleration": result["peak_acceleration"],
        "variability": result["variability"],
        "post_activity": result["post_activity"],
        "confirmation_status": confirmation_status,
        "caregiver_notification_status": notification_status,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    return {
        "status": "success",
        "event_id": event_id,
        "event_type": event_type,
        "confirmation_status": confirmation_status,
        "caregiver_notification_status": notification_status,
        "peak_acceleration": result["peak_acceleration"],
        "variability": result["variability"],
        "reason": result["reason"],
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "specification": "ElderGuard Final System Blueprint v1.0 & IT Implementation Brief v1.0",
        "storage": "supabase" if supabase is not None else "local_csv_only",
    }