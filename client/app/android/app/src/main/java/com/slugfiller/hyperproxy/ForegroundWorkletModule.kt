package com.slugfiller.hyperproxy

import android.Manifest
import android.content.Context
import android.content.Intent
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class ForegroundWorkletModule(reactContext: ReactApplicationContext) : NativeForegroundWorkletSpec(reactContext) {

  override fun getName() = NAME

  override fun startService(): Unit {
    val context: Context = getReactApplicationContext().getApplicationContext()

    // No need to actually check the return value here
    // This is only called to give the chance for the permission to be present
    // Even if disallowed, we can continue as normal
    context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS)

    ForegroundService.createChannel(context)

    val intent = Intent(context, ForegroundService::class.java)
    context.startForegroundService(intent)
  }

  override fun readMain(result: Promise) {
    var ret: String
    var other: Promise
    stateLock.withLock {
      if (promiseWriteWorklet == null) {
        promiseReadMain = result
        return
      }
      ret = bufferWorklet
      other = promiseWriteWorklet!!
      bufferWorklet = ""
      promiseWriteWorklet = null
    }
    result.resolve(ret)
    other.resolve(null)
  }

  override fun writeMain(buffer: String, result: Promise) {
    var other: Promise
    stateLock.withLock {
      if (promiseReadWorklet == null) {
        promiseWriteMain = result
        bufferMain = buffer
        return
      }
      other = promiseReadWorklet!!
	  promiseReadWorklet = null
    }
    other.resolve(buffer)
    result.resolve(null)
  }

  override fun readWorklet(result: Promise) {
    var ret: String
    var other: Promise
    stateLock.withLock {
      if (promiseWriteMain == null) {
        promiseReadWorklet = result
        return
      }
      ret = bufferMain
      other = promiseWriteMain!!
      bufferMain = ""
      promiseWriteMain = null
    }
    result.resolve(ret)
    other.resolve(null)
  }

  override fun writeWorklet(buffer: String, result: Promise) {
    var other: Promise
    stateLock.withLock {
      if (promiseReadMain == null) {
        promiseWriteWorklet = result
        bufferWorklet = buffer
        return
      }
      other = promiseReadMain!!
	  promiseReadMain = null
    }
    other.resolve(buffer)
    result.resolve(null)
  }

  companion object {
    const val NAME = NativeForegroundWorkletSpec.NAME

    private val stateLock = ReentrantLock()

    @Volatile private var promiseWriteMain: Promise? = null
    @Volatile private var promiseReadMain: Promise? = null
    @Volatile private var bufferMain: String = ""

    @Volatile private var promiseWriteWorklet: Promise? = null
    @Volatile private var promiseReadWorklet: Promise? = null
    @Volatile private var bufferWorklet: String = ""
  }
}
