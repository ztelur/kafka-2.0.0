/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils.timer

import java.util.concurrent.{DelayQueue, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

/**
  * 上面介绍的 TimingWheel 提供了添加延时任务和推进时间轮指针的操作，
  * 而具体执行延时任务的操作则交由定时器 SystemTimer 完成。
  * SystemTimer 类实现了 Timer 特质，该特质描绘了定时器应该具备的基本方法
  */
trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
*         添加延时任务，如果任务到期则会立即触发执行
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    * 推动时间轮指针，期间会执行已经到期的任务
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    * 获取时间轮中等待被调度的任务数
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    * 关闭定时器，丢弃未执行的延时任务
    */
  def shutdown(): Unit
}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1, // 默认时间格时间为 1 毫秒
                  wheelSize: Int = 20, // 默认时间格大小为 20
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer { // 时间轮启动时间戳

  // timeout timer
  /**
    * 延时任务执行线程池
    */
  private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    def newThread(runnable: Runnable): Thread =
      KafkaThread.nonDaemon("executor-"+executorName, runnable)
  })
  /**
    * 各层级时间轮共用的延时任务队列
    */
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  /**
    * 各层级时间轮共用的任务计数器
    */
  private[this] val taskCounter = new AtomicInteger(0)
  /**
    * 分层时间轮中最底层的时间轮
    */
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()


  /**
    * 方法 SystemTimer#add 会将待添加的延时任务 TimerTask 对象封装成 TimerTaskEntry 对象添加到对应的时间格中，
    * 添加的过程调用的是 TimingWheel#add 方法。前面曾介绍过该方法会将未到期的延时任务添加到对应的时间轮中并返回 true，
    * 对于已到期或已经被取消的延时任务则会立即返回 false。由下面的实现可以看到，对于那些已经到期但是未被取消的任务，
    * 会立即被提交给执行线程予以执行。
    * @param timerTask the task to add
    *         添加延时任务，如果任务到期则会立即触发执行
    */
  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      /**
        * 将 TimerTask 封装成 TimerTaskEntry 对象，并添加到时间轮中
        */
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    /**
      * 往时间轮中添加延时任务，同时检测添加的任务是否已经到期
      */
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      /**
        * 任务到期但未被取消，则立即提交执行
        */
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   * 推进时间轮指针，同时处理时间格中到期的任务
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    /**
      * 超时等待获取时间格对象
      */
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          /**
            * 推进时间轮指针，对应的时间戳为当前时间格时间区间上界
            */
          timingWheel.advanceClock(bucket.getExpiration())

          /**
            * 遍历处理当前时间格中的延时任务，提交执行到期但未被取消的任务，
            * 对于未到期的任务重新添加到时间轮中继续等待被执行，期间可能会对任务在层级上执行降级
            *
            *
            * 这里需要清楚的一点是，从当前时间格中移出但未到期的延时任务，当再次被添加到时间轮中时，
            * 不一定会被添加到原来的时间轮中，因为随着时间的流失，距离对应延时任务的时间差也越来越小，
            * 这个时候一般会发生时间轮的降级，即从一个较大（时间区间）粒度的时间轮中降落到粒度较小的时间轮中。
            * 实际上，从在时间轮中等待到被执行本质上也是一种降级操作，只是这里较小的时间粒度是 0，
            * 表示延时任务已经到期，需要立即被执行。
            */
          bucket.flush(reinsert)
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown() {
    taskExecutor.shutdown()
  }

}

