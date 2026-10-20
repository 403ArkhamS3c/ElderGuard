package com.elderguard.watch

import android.app.Activity
import android.content.Context
import android.graphics.Color
import android.hardware.Sensor
import android.hardware.SensorEvent
import android.hardware.SensorEventListener
import android.hardware.SensorManager
import android.os.Bundle
import android.os.CountDownTimer
import android.util.Log
import android.view.View
import android.view.WindowManager
import android.widget.Button
import android.widget.LinearLayout
import android.widget.TextView
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import java.io.IOException
import java.util.Locale
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.TimeUnit
import kotlin.math.sqrt

class MainActivity : Activity(), SensorEventListener {

    companion object {
        private const val TAG = "ELDER_DEBUG"
    }

    // --- Konfigurasi Jaringan ---
    private var webSocket: WebSocket? = null
    private val client = OkHttpClient.Builder()
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .build()

    private val serverWsUrl = "ws://192.168.0.133:8000/ws/sensor/S01"
    private val serverHttpUrl = "http://192.168.0.133:8000/api/events/confirm"

    // --- Status Internal ---
    @Volatile private var isWsConnected = false
    @Volatile private var isSensorRunning = false
    @Volatile private var isWaitingUserResponse = false

    private var countdownTimer: CountDownTimer? = null

    // --- Komponen UI Utama ---
    private lateinit var layoutMain: LinearLayout
    private lateinit var tvStatus: TextView
    private lateinit var tvMetrics: TextView
    private lateinit var btnStart: Button
    private lateinit var btnStop: Button

    // --- Komponen UI Konfirmasi (Post-Event Response) ---
    private lateinit var layoutAlert: LinearLayout
    private lateinit var tvAlertTitle: TextView
    private lateinit var tvAlertTimer: TextView
    private lateinit var btnOk: Button
    private lateinit var btnHelp: Button

    // --- Konfigurasi Sensor ---
    private lateinit var sensorManager: SensorManager
    private var accelerometer: Sensor? = null
    private var barometer: Sensor? = null

    @Volatile private var currentAx = 0f
    @Volatile private var currentAy = 0f
    @Volatile private var currentAz = 0f
    @Volatile private var currentPressure = 0f

    private var streamingTimer: Timer? = null
    private var packetCounter = 0

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)

        // Inisialisasi UI Utama
        layoutMain = findViewById(R.id.layoutMain)
        tvStatus = findViewById(R.id.tvStatus)
        tvMetrics = findViewById(R.id.tvMetrics)
        btnStart = findViewById(R.id.btnStart)
        btnStop = findViewById(R.id.btnStop)

        // Inisialisasi UI Alert
        layoutAlert = findViewById(R.id.layoutAlert)
        tvAlertTitle = findViewById(R.id.tvAlertTitle)
        tvAlertTimer = findViewById(R.id.tvAlertTimer)
        btnOk = findViewById(R.id.btnOk)
        btnHelp = findViewById(R.id.btnHelp)

        sensorManager = getSystemService(Context.SENSOR_SERVICE) as SensorManager
        accelerometer = sensorManager.getDefaultSensor(Sensor.TYPE_ACCELEROMETER)
        barometer = sensorManager.getDefaultSensor(Sensor.TYPE_PRESSURE)

        connectWebSocket()

        btnStart.setOnClickListener {
            if (!isWsConnected) {
                setStatusUi("Reconnecting...", "#D29922")
                connectWebSocket()
            }
            startSensorsManually()
        }

        btnStop.setOnClickListener {
            stopSensorsManually()
        }
    }

    private fun setStatusUi(text: String, hexColor: String) {
        runOnUiThread {
            tvStatus.text = text
            tvStatus.setTextColor(Color.parseColor(hexColor))
        }
    }

    private fun startSensorsManually() {
        if (isSensorRunning) return
        isSensorRunning = true
        isWaitingUserResponse = false
        packetCounter = 0

        val samplingPeriodUs = 40_000 // 25 Hz
        accelerometer?.let { sensorManager.registerListener(this, it, samplingPeriodUs) }
        barometer?.let { sensorManager.registerListener(this, it, samplingPeriodUs) }

        startStreamingData()

        // Sembunyikan tombol START, tampilkan tombol STOP di tempat yang sama
        btnStart.visibility = View.GONE
        btnStop.visibility = View.VISIBLE
        btnStop.isEnabled = true
        setStatusUi("Monitoring Active", "#3FB950")
    }

    private fun stopSensorsManually() {
        isSensorRunning = false
        isWaitingUserResponse = false

        sensorManager.unregisterListener(this)
        stopStreamingData()

        // Tampilkan kembali tombol START, sembunyikan tombol STOP
        btnStop.visibility = View.GONE
        btnStart.visibility = View.VISIBLE
        btnStart.isEnabled = true
        tvMetrics.text = "ACC: 0.00 G  |  P: -- hPa"

        if (isWsConnected) {
            setStatusUi("Ready to Start", "#FFFFFF")
        } else {
            setStatusUi("Disconnected", "#F85149")
        }
    }

    override fun onSensorChanged(event: SensorEvent?) {
        if (event == null) return
        when (event.sensor.type) {
            Sensor.TYPE_ACCELEROMETER -> {
                currentAx = event.values[0]
                currentAy = event.values[1]
                currentAz = event.values[2]
            }
            Sensor.TYPE_PRESSURE -> {
                currentPressure = event.values[0]
            }
        }
    }

    override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) {}

    private fun startStreamingData() {
        if (streamingTimer != null) return

        streamingTimer = Timer()
        streamingTimer?.scheduleAtFixedRate(object : TimerTask() {
            override fun run() {
                if (!isSensorRunning || isWaitingUserResponse) return
                if (currentPressure == 0f || !isWsConnected) return

                val nowMs = System.currentTimeMillis()
                val sensorData = JSONObject().apply {
                    put("timestamp", nowMs)
                    put("ax", currentAx)
                    put("ay", currentAy)
                    put("az", currentAz)
                    put("pressure", currentPressure)
                }

                val isSent = webSocket?.send(sensorData.toString()) ?: false
                if (isSent) {
                    packetCounter++
                    if (packetCounter % 15 == 0) {
                        val gForce = sqrt(
                            (currentAx * currentAx + currentAy * currentAy + currentAz * currentAz).toDouble()
                        ) / 9.80665

                        runOnUiThread {
                            if (isSensorRunning && !isWaitingUserResponse) {
                                tvMetrics.text = String.format(
                                    Locale.US,
                                    "ACC: %.2f G  |  %.0f hPa",
                                    gForce, currentPressure
                                )
                            }
                        }
                    }
                }
            }
        }, 0, 40)
    }

    private fun stopStreamingData() {
        streamingTimer?.cancel()
        streamingTimer = null
    }

    private fun connectWebSocket() {
        val request = Request.Builder().url(serverWsUrl).build()
        webSocket = client.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                isWsConnected = true
                if (!isSensorRunning) {
                    setStatusUi("Ready to Start", "#FFFFFF")
                } else {
                    setStatusUi("Monitoring Active", "#3FB950")
                }
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                try {
                    val json = JSONObject(text)
                    if (json.getString("type") == "POST_EVENT_CHECK") {
                        val eventId = json.getString("event_id")
                        val classification = json.optString("classification", "Near-Fall")
                        runOnUiThread {
                            showPostEventScreen(eventId, classification)
                        }
                    }
                } catch (e: Exception) {
                    Log.e(TAG, "JSON Parse Error", e)
                }
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                isWsConnected = false
                setStatusUi("Disconnected", "#F85149")
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                isWsConnected = false
                setStatusUi("Connection Failed", "#F85149")
            }
        })
    }

    // Menampilkan Layar Konfirmasi Penuh (Lebih jelas untuk Lansia di Smartwatch)
    private fun showPostEventScreen(eventId: String, classification: String) {
        isWaitingUserResponse = true
        countdownTimer?.cancel()

        // Ganti tampilan dari Layar Utama ke Layar Konfirmasi
        layoutMain.visibility = View.GONE
        layoutAlert.visibility = View.VISIBLE

        tvAlertTitle.text = "${classification.uppercase()} DETECTED"
        tvAlertTitle.setTextColor(
            if (classification == "Fall") Color.parseColor("#F85149") else Color.parseColor("#D29922")
        )

        btnOk.setOnClickListener {
            countdownTimer?.cancel()
            sendResponseToServer(eventId, "OK", classification)
        }

        btnHelp.setOnClickListener {
            countdownTimer?.cancel()
            sendResponseToServer(eventId, "NEED_HELP", classification)
        }

        countdownTimer = object : CountDownTimer(15000, 1000) {
            override fun onTick(millisUntilFinished: Long) {
                tvAlertTimer.text = "Are you OK? (${millisUntilFinished / 1000}s)"
            }

            override fun onFinish() {
                sendResponseToServer(eventId, "NO_RESPONSE", classification)
            }
        }.start()
    }

    private fun sendResponseToServer(eventId: String, responseType: String, classification: String) {
        // Kembalikan layar ke tampilan monitoring utama
        layoutAlert.visibility = View.GONE
        layoutMain.visibility = View.VISIBLE
        isWaitingUserResponse = false

        val jsonPayload = JSONObject().apply {
            put("event_id", eventId)
            put("response_type", responseType)
            put("classification", classification)
        }.toString()

        val body = jsonPayload.toRequestBody("application/json; charset=utf-8".toMediaTypeOrNull())
        val request = Request.Builder()
            .url(serverHttpUrl)
            .post(body)
            .build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                Log.e(TAG, "Gagal mengirim respons konfirmasi", e)
            }
            override fun onResponse(call: Call, response: Response) {
                Log.d(TAG, "Respons terkirim: $responseType")
            }
        })
    }

    override fun onDestroy() {
        super.onDestroy()
        stopStreamingData()
        sensorManager.unregisterListener(this)
        countdownTimer?.cancel()
        webSocket?.close(1000, "Aplikasi ditutup")
    }
}