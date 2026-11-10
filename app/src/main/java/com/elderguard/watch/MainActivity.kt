package com.elderguard.watch

import android.app.Activity
import android.content.Context
import android.graphics.Color
import android.hardware.Sensor
import android.hardware.SensorEvent
import android.hardware.SensorEventListener
import android.hardware.SensorManager
import android.os.Build
import android.os.Bundle
import android.os.CountDownTimer
import android.os.VibrationEffect
import android.os.Vibrator
import android.os.VibratorManager
import android.util.Log
import android.view.View
import android.view.WindowManager
import android.widget.Button
import android.widget.LinearLayout
import android.widget.TextView
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.Timer
import java.util.TimerTask
import java.util.UUID
import kotlin.math.abs
import kotlin.math.sqrt

class MainActivity : Activity(), SensorEventListener {

    companion object {
        private const val TAG = "ELDERGUARD_WATCH"
        
        // Spesifikasi Resmi Blueprint v1.0 & IT Brief v1.0
        private const val SAMPLING_RATE_HZ = 50                     // Target 50 Hz[cite: 3, 4]
        private const val SAMPLE_INTERVAL_MS = 20L                  // ~20 ms interval[cite: 4]
        private const val OBSERVATION_WINDOW_SEC = 3                // 3 detik observation window[cite: 3, 4]
        private const val TARGET_SAMPLES = SAMPLING_RATE_HZ * OBSERVATION_WINDOW_SEC // 150 sampel[cite: 3, 4]
        private const val TRIGGER_THRESHOLD_G = 2.0                 // Dynamic acceleration trigger[cite: 4]
        
        private const val GRAVITY_ALPHA = 0.0353f                   // Low-pass filter ~0.3 Hz
    }

    private val client = OkHttpClient()
    private val serverEvaluateUrl = "https://elderguard.fly.dev/api/events/evaluate"

    // UI Utama
    private lateinit var layoutMain: LinearLayout
    private lateinit var tvStatus: TextView
    private lateinit var tvMetrics: TextView
    private lateinit var btnStart: Button
    private lateinit var btnStop: Button

    // UI Human Confirmation (30s Timeout)[cite: 3, 4]
    private lateinit var layoutAlert: LinearLayout
    private lateinit var tvAlertTitle: TextView
    private lateinit var tvAlertTimer: TextView
    private lateinit var btnYes: Button
    private lateinit var btnNo: Button

    private lateinit var sensorManager: SensorManager
    private var accelerometer: Sensor? = null
    private var vibrator: Vibrator? = null

    @Volatile private var isMonitoring = false
    @Volatile private var isObservingWindow = false
    @Volatile private var isUserResponding = false

    @Volatile private var currentAx = 0f
    @Volatile private var currentAy = 0f
    @Volatile private var currentAz = 0f

    private var estimatedGravity = 1.0f
    private val observationSamples = ArrayList<SensorSample>()

    private data class SensorSample(val x: Float, val y: Float, val z: Float, val timestamp: Long)

    private var samplingTimer: Timer? = null
    private var countDownTimer: CountDownTimer? = null
    private var uiTick = 0

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)

        layoutMain = findViewById(R.id.layoutMain)
        tvStatus = findViewById(R.id.tvStatus)
        tvMetrics = findViewById(R.id.tvMetrics)
        btnStart = findViewById(R.id.btnStart)
        btnStop = findViewById(R.id.btnStop)

        layoutAlert = findViewById(R.id.layoutAlert)
        tvAlertTitle = findViewById(R.id.tvAlertTitle)
        tvAlertTimer = findViewById(R.id.tvAlertTimer)
        btnYes = findViewById(R.id.btnOk)
        btnNo = findViewById(R.id.btnHelp)

        sensorManager = getSystemService(Context.SENSOR_SERVICE) as SensorManager
        accelerometer = sensorManager.getDefaultSensor(Sensor.TYPE_ACCELEROMETER)
        
        vibrator = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            val vibratorManager = getSystemService(Context.VIBRATOR_MANAGER_SERVICE) as VibratorManager
            vibratorManager.defaultVibrator
        } else {
            @Suppress("DEPRECATION")
            getSystemService(Context.VIBRATOR_SERVICE) as Vibrator
        }

        setStatusUi("Ready (50 Hz)", "#FFFFFF")
        tvMetrics.text = "A_dynamic: 0.00G | Monitoring"

        btnStart.setOnClickListener { startMonitoring() }
        btnStop.setOnClickListener { stopMonitoring() }
    }

    private fun setStatusUi(text: String, hexColor: String) {
        runOnUiThread {
            tvStatus.text = text
            tvStatus.setTextColor(Color.parseColor(hexColor))
        }
    }

    private fun startMonitoring() {
        if (isMonitoring) return
        isMonitoring = true
        isObservingWindow = false
        isUserResponding = false
        observationSamples.clear()
        estimatedGravity = 1.0f

        accelerometer?.let {
            sensorManager.registerListener(this, it, 20_000) // 50 Hz
        }

        startSamplingLoop()

        btnStart.visibility = View.GONE
        btnStop.visibility = View.VISIBLE
        btnStop.isEnabled = true
        setStatusUi("Monitoring (50 Hz)", "#3FB950")
    }

    private fun stopMonitoring() {
        isMonitoring = false
        isObservingWindow = false
        isUserResponding = false

        sensorManager.unregisterListener(this)
        samplingTimer?.cancel()
        samplingTimer = null
        countDownTimer?.cancel()

        btnStop.visibility = View.GONE
        btnStart.visibility = View.VISIBLE
        btnStart.isEnabled = true
        setStatusUi("Stopped", "#F85149")
        tvMetrics.text = "A_dynamic: 0.00G | Stopped"
    }

    override fun onSensorChanged(event: SensorEvent?) {
        if (event?.sensor?.type == Sensor.TYPE_ACCELEROMETER) {
            currentAx = event.values[0]
            currentAy = event.values[1]
            currentAz = event.values[2]
        }
    }

    override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) {}

    // =========================================================================
    // CONTINUOUS MONITORING & TRIGGER DETECTION (IT BRIEF SECTION 4 & 6)
    // =========================================================================
    private fun startSamplingLoop() {
        samplingTimer?.cancel()
        samplingTimer = Timer()
        samplingTimer?.scheduleAtFixedRate(object : TimerTask() {
            override fun run() {
                if (!isMonitoring || isUserResponding) return

                val ax = currentAx
                val ay = currentAy
                val az = currentAz
                val now = System.currentTimeMillis()

                val aMagnitudeMs2 = sqrt((ax * ax + ay * ay + az * az).toDouble())
                val aMagnitudeG = (aMagnitudeMs2 / 9.80665f).toFloat()

                // Estimasi gravitasi & kalkulasi akselerasi dinamis[cite: 3, 4]
                estimatedGravity = GRAVITY_ALPHA * aMagnitudeG + (1 - GRAVITY_ALPHA) * estimatedGravity
                val aDynamic = abs(aMagnitudeG - estimatedGravity)

                val sample = SensorSample(ax, ay, az, now)

                if (!isObservingWindow) {
                    // Trigger Condition: A_d(t) >= T_TRIGGER (2.0 G)[cite: 4]
                    if (aDynamic >= TRIGGER_THRESHOLD_G) {
                        isObservingWindow = true
                        observationSamples.clear()
                        observationSamples.add(sample)

                        Log.i(TAG, "Trigger terdeteksi! A_dynamic=${String.format(Locale.US, "%.2f", aDynamic)}G. Memulai observasi 3s...")
                        setStatusUi("Observing (3s)...", "#D29922")
                    } else {
                        uiTick++
                        if (uiTick % 25 == 0) {
                            runOnUiThread {
                                if (isMonitoring && !isObservingWindow && !isUserResponding) {
                                    tvMetrics.text = String.format(Locale.US, "A_dyn: %.2fG | Monitoring", aDynamic)
                                }
                            }
                        }
                    }
                } else {
                    // 3-second Post-Trigger Observation (150 samples)[cite: 3, 4]
                    observationSamples.add(sample)

                    val count = observationSamples.size
                    if (count % 25 == 0) {
                        runOnUiThread {
                            tvMetrics.text = "Observation: $count / $TARGET_SAMPLES"
                        }
                    }

                    if (count >= TARGET_SAMPLES) {
                        isObservingWindow = false
                        val windowCopy = ArrayList(observationSamples)
                        
                        setStatusUi("Evaluating...", "#58A6FF")
                        sendObservationWindowToBackend(windowCopy)
                    }
                }
            }
        }, 0, SAMPLE_INTERVAL_MS)
    }

    private fun sendObservationWindowToBackend(observationData: List<SensorSample>) {
        val eventId = UUID.randomUUID().toString()
        val timestampStr = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US).format(Date())

        val xList = JSONArray()
        val yList = JSONArray()
        val zList = JSONArray()

        for (s in observationData) {
            xList.put(s.x)
            yList.put(s.y)
            zList.put(s.z)
        }

        // Payload disesuaikan dengan skema Blueprint v1.0 menggunakan observation_window[cite: 3, 4]
        val payloadJson = JSONObject().apply {
            put("event_id", eventId)
            put("user_id", "00000000-0000-0000-0000-000000000000")
            put("device_id", "Samsung-Galaxy-Watch4")
            put("timestamp", timestampStr)
            put("observation_window", JSONObject().apply {
                put("x", xList)
                put("y", yList)
                put("z", zList)
            })
        }

        val body = payloadJson.toString().toRequestBody("application/json; charset=utf-8".toMediaTypeOrNull())
        val request = Request.Builder()
            .url(serverEvaluateUrl)
            .post(body)
            .build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                Log.e(TAG, "Gagal mengirim data event ke backend: ${e.message}", e)
                setStatusUi("Network Error", "#F85149")
            }

            override fun onResponse(call: Call, response: Response) {
                val respStr = response.body?.string() ?: ""
                Log.i(TAG, "Respon Klasifikasi Server: $respStr")
                try {
                    val json = JSONObject(respStr)
                    val eventType = json.optString("event_type", "NORMAL")
                    
                    runOnUiThread {
                        when (eventType) {
                            "NEAR_FALL" -> showHumanConfirmationInterface("NEAR_FALL")
                            "FALL" -> showHumanConfirmationInterface("FALL")
                            else -> setStatusUi("Monitoring (50 Hz)", "#3FB950")
                        }
                    }
                } catch (e: Exception) {
                    Log.e(TAG, "Parse JSON error", e)
                    setStatusUi("Monitoring (50 Hz)", "#3FB950")
                }
            }
        })
    }

    // =========================================================================
    // HUMAN CONFIRMATION SYSTEM (30s TIMEOUT - BLUEPRINT SECTION 10)[cite: 3, 4]
    // =========================================================================
    private fun showHumanConfirmationInterface(eventType: String) {
        isUserResponding = true
        countDownTimer?.cancel()

        vibrateDevice(longArrayOf(0, 300, 100, 300), true)

        layoutMain.visibility = View.GONE
        layoutAlert.visibility = View.VISIBLE

        if (eventType == "FALL") {
            tvAlertTitle.text = "Did you fall?"
            tvAlertTitle.setTextColor(Color.parseColor("#F85149"))
        } else {
            tvAlertTitle.text = "Did you experience a near-fall?"
            tvAlertTitle.setTextColor(Color.parseColor("#D29922"))
        }

        btnYes.text = "YES"
        btnYes.setBackgroundColor(Color.parseColor("#2EA043"))

        btnNo.text = "NO"
        btnNo.setBackgroundColor(Color.parseColor("#DA3633"))

        btnYes.setOnClickListener {
            countDownTimer?.cancel()
            dismissConfirmation("CONFIRMED") // YES -> CONFIRMED[cite: 3, 4]
        }

        btnNo.setOnClickListener {
            countDownTimer?.cancel()
            dismissConfirmation("FALSE_EVENT") // NO -> FALSE_EVENT[cite: 3, 4]
        }

        // Timeout 30 detik sesuai spesifikasi resmi[cite: 3, 4]
        countDownTimer = object : CountDownTimer(30000, 1000) {
            override fun onTick(millisUntilFinished: Long) {
                tvAlertTimer.text = "Timeout: ${millisUntilFinished / 1000}s"
            }

            override fun onFinish() {
                dismissConfirmation("UNCONFIRMED") // Timeout -> UNCONFIRMED[cite: 3, 4]
            }
        }.start()
    }

    private fun dismissConfirmation(confirmationStatus: String) {
        vibrator?.cancel()
        layoutAlert.visibility = View.GONE
        layoutMain.visibility = View.VISIBLE
        isUserResponding = false

        setStatusUi("Monitoring (50 Hz)", "#3FB950")
        Log.i(TAG, "Human Confirmation Status Recorded: $confirmationStatus")
    }

    private fun vibrateDevice(pattern: LongArray, repeat: Boolean) {
        if (vibrator?.hasVibrator() == true) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                vibrator?.vibrate(VibrationEffect.createWaveform(pattern, if (repeat) 0 else -1))
            } else {
                @Suppress("DEPRECATION")
                vibrator?.vibrate(pattern, if (repeat) 0 else -1)
            }
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        vibrator?.cancel()
        stopMonitoring()
    }
}