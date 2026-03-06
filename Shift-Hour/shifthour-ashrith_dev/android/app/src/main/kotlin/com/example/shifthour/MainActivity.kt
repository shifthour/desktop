package com.shifthour.app

import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.os.Build
import android.os.Bundle
import android.util.Log
import io.flutter.embedding.android.FlutterActivity

class MainActivity : FlutterActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        Log.d("MainActivity", "onCreate called ✅")

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channelId = "shifthour_general"
            val channelName = "ShiftHour Notifications"
            val description = "Used for shift alerts, job posts, and updates"
            val importance = NotificationManager.IMPORTANCE_HIGH

            val channel = NotificationChannel(channelId, channelName, importance).apply {
                this.description = description
            }

            val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            manager.createNotificationChannel(channel)

            Log.d("ChannelRegistration", "✅ Registered notification channel: $channelId")
        }
    }
}
