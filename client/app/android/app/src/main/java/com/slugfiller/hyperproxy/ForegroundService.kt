package com.slugfiller.hyperproxy

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.IBinder
import com.facebook.react.HeadlessJsTaskService
import com.facebook.react.bridge.WritableNativeMap
import com.facebook.react.jstasks.HeadlessJsTaskConfig
import java.util.concurrent.atomic.AtomicBoolean

class ForegroundService : HeadlessJsTaskService() {

  override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
    val notificationBuilder = Notification.Builder(applicationContext, NOTIFICATION_CHANNEL_ID)
      .setContentTitle("HyperProxy")
      .setContentText("Proxy service running")
      .setCategory(Notification.CATEGORY_SERVICE)
    startForeground(1, notificationBuilder.build(), ServiceInfo.FOREGROUND_SERVICE_TYPE_REMOTE_MESSAGING)

    return super.onStartCommand(intent, flags, startId)
  }

  override fun getTaskConfig(intent: Intent?): HeadlessJsTaskConfig? = HeadlessJsTaskConfig(
    "ForegroundWorklet",
    WritableNativeMap(),
    0,
    true
  )

  companion object {
    const val NOTIFICATION_CHANNEL_ID = "com.slugfiller.hyperproxy.ForegroundService"
    const val NOTIFICATION_CHANNEL_NAME = "HyperProxy"

    private val created = AtomicBoolean(false)

    fun createChannel(context: Context) {
      if (!created.compareAndSet(false, true)) {
        return
      }
      val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
      val channel = NotificationChannel(NOTIFICATION_CHANNEL_ID, NOTIFICATION_CHANNEL_NAME, NotificationManager.IMPORTANCE_LOW)
      manager.createNotificationChannel(channel)
    }
  }
}
