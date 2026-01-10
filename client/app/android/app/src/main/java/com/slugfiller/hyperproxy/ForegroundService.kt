/**
 * BSD Zero Clause License
 *
 * Permission to use, copy, modify, and/or distribute this software for
 * any purpose with or without fee is hereby granted.
 *
 * THE SOFTWARE IS PROVIDED “AS IS” AND THE AUTHOR DISCLAIMS ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE
 * FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY
 * DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

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
