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

package kafka.server

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.{Lock, ReentrantLock, ReentrantReadWriteLock}

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.utils.timer._

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 */
abstract class DelayedOperation(override val delayMs: Long,
    lockOpt: Option[Lock] = None) extends TimerTask with Logging {

  private val completed = new AtomicBoolean(false)
  private val tryCompletePending = new AtomicBoolean(false)
  // Visible for testing
  private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)

  /*
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   *
   * 强制执行延时任务，包括满足执行条件主动触发，以及延时到期
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) { //// CAS 操作修改 completed 字段
      // cancel the timeout timer
      cancel() // 将当前延时任务从时间轮中移除
      onComplete() // 立即触发执行延时任务
      true
    } else {
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
    * 检测延时任务是否完成执行。
   */
  def isCompleted: Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
    * 当延时任务因为时间到期被执行时会触发该方法中定义的逻辑。
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
    * 延时任务的具体执行逻辑，在整个延时任务的生命周期中只能被调用一次，且只能由 forceComplete 方法调用。
   */
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
    *
    * 检测是否满足延时任务执行条件，若满足则会调用 forceComplete 方法。
   */
  def tryComplete(): Boolean

  /**
   * Thread-safe variant of tryComplete() that attempts completion only if the lock can be acquired
   * without blocking.
   *
   * If threadA acquires the lock and performs the check for completion before completion criteria is met
   * and threadB satisfies the completion criteria, but fails to acquire the lock because threadA has not
   * yet released the lock, we need to ensure that completion is attempted again without blocking threadA
   * or threadB. `tryCompletePending` is set by threadB when it fails to acquire the lock and at least one
   * of threadA or threadB will attempt completion of the operation if this flag is set. This ensures that
   * every invocation of `maybeTryComplete` is followed by at least one invocation of `tryComplete` until
   * the operation is actually completed.
   */
  private[server] def maybeTryComplete(): Boolean = {
    var retry = false
    var done = false
    do {
      if (lock.tryLock()) {
        try {
          tryCompletePending.set(false)
          done = tryComplete()
        } finally {
          lock.unlock()
        }
        // While we were holding the lock, another thread may have invoked `maybeTryComplete` and set
        // `tryCompletePending`. In this case we should retry.
        retry = tryCompletePending.get()
      } else {
        // Another thread is holding the lock. If `tryCompletePending` is already set and this thread failed to
        // acquire the lock, then the thread that is holding the lock is guaranteed to see the flag and retry.
        // Otherwise, we should set the flag and retry on this thread since the thread holding the lock may have
        // released the lock and returned by the time the flag is set.
        retry = !tryCompletePending.getAndSet(true)
      }
    } while (!isCompleted && retry)
    done
  }

  /*
   * run() method defines a task that is executed on timeout
   * DelayedOperation 实现了 TimerTask 特质，所以也间接实现了
   * Runnable 接口，当延时任务到期时会被提交给定时器的线程执行
   *
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

/**
  * DelayedOperationPurgatory 类提供了对 DelayedOperation 管理的功能，
  * DelayedOperationPurgatory 维护了一个 Pool 类型（key/value 类型）的 watchersForKey 对象，
  * 用于记录延时任务及其关注的 key 之间的映射关系，用于支持时间维度以外的其它维度操作。
  * Pool 封装了 ConcurrentHashMap，并在 ConcurrentHashMap 的基础上添加了 Pool#getAndMaybePut 方法，
  * 用于在对应 key 不命中时使用给定的 value 更新键值对。Pool 的默认构造方法接收一个
  * Option[K => V] 类型的 valueFactory 参数，用于为 key 生成对应的 Watchers 对象
  */
object DelayedOperationPurgatory {

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                             timeoutTimer: Timer, // 定时器
                                                             brokerId: Int = 0,
                                                             purgeInterval: Int = 1000,
                                                             reaperEnabled: Boolean = true,
                                                             timerEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys */
  /**
    * 用于管理 DelayedOperation，其中 key 是 Watcher 中的 DelayedOperation 集合所关心的对象
    */
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))
  /**
    * watchersForKey 读写锁
    */
  private val removeWatchersLock = new ReentrantReadWriteLock()

  // the number of estimated total operations in the purgatory
  /**
    * 记录当前 DelayedOperationPurgatory 中延时任务的个数
    */
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  /**
    * 主要具备 2 个作用：
    *   1. 推进时间指针
    *   2. 定期清理 watchersForKey 中已经完成的延时任务
    *
    */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value: Int = watched
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value: Int = delayed
    },
    metricsTags
  )

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
    *
    *        用于往 DelayedOperationPurgatory 中添加延时任务，一个延时任务可以被同时关联到多个 key 对象上，
    *        这样可以从多个维度触发执行该延时任务
    *
    *
    *        注册延时任务的执行流程如下：
    *
    *        尝试执行延时任务，如果当前已经完成执行则返回；
    *        否则，遍历给定的 key 集合，将延时任务添加到每个 key 的 Watchers 中，建立从多个维度触发延时任务执行的条件，期间如果延时任务已经完成执行，则不再继续添加；
    *        再次尝试执行延时任务，如果当前已经完成执行则返回；
    *        否则，将延时任务添加到定时器中，建立从时间维度触发延时任务执行的条件，如果期间任务已经完成执行，则从时间轮中取消任务。
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.

    // At this point the only thread that can attempt this operation is this current thread
    // Hence it is safe to tryComplete() without a lock
    /**
      *  1。 调用延时任务的 tryComplete 方法，尝试完成延迟操作
      */
    var isCompletedByMe = operation.tryComplete()

    /**
      * 如果延时任务已经执行完成，则直接返回
      */
    if (isCompletedByMe)
      return true

    var watchCreated = false
    for(key <- watchKeys) {
      // If the operation is already completed, stop adding it to the rest of the watcher list.
      if (operation.isCompleted)
        return false

      /**
        * 遍历处理 watchKeys，将延时任务添加其关心的 key 对应的 Watchers 中
        */
      watchForOperation(key, operation)

      if (!watchCreated) {
        watchCreated = true

        /**
          * 延时任务计数加 1，一个延时任务可能会被添加到多个 key 对应的 Watchers 集合中，但是任务计数只会增加 1 次
          */
        estimatedTotalOperations.incrementAndGet()
      }
    }

    /**
      * 再次调用延时任务的 tryComplete 方法，尝试完成延迟操作
      */
    isCompletedByMe = operation.maybeTryComplete()
    if (isCompletedByMe)
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    /**
      * 对于未执行的延时任务，尝试添加到定时器中，用于从时间维度触发延时任务执行
      */
    if (!operation.isCompleted) {
      if (timerEnabled)
        timeoutTimer.add(operation)

      /**
        * 再次检测延时任务的执行情况，如果已经完成则从定时器中移除
        */
      if (operation.isCompleted) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    /**
      * 获取 key 对应的 Watchers 对象
      */
    val watchers = inReadLock(removeWatchersLock) { watchersForKey.get(key) }
    if(watchers == null)
      0
    else

    /**
      * 如果存在对应的 Watchers 对象，则对记录在其中待执行的延时任务尝试触发执行，并移除已经执行完成的任务
      */
      watchers.tryCompleteWatched()
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched: Int = allWatchers.map(_.countWatched).sum

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def delayed: Int = timeoutTimer.size

  /**
    * Cancel watching on any delayed operations for the given key. Note the operation will not be completed
    */
  def cancelForKey(key: Any): List[T] = {
    inWriteLock(removeWatchersLock) {
      val watchers = watchersForKey.remove(key)
      if (watchers != null)
        watchers.cancel()
      else
        Nil
    }
  }
  /*
   * Return all the current watcher lists,
   * note that the returned watchers may be removed from the list by other threads
   */
  private def allWatchers = inReadLock(removeWatchersLock) { watchersForKey.values }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   * 添加延时任务添加到对应 key 的 Watchers 集合中，用于从时间维度以外的维度触发延时任务执行
   *
   * 尝试获取 key 对应的 Watchers 对象，如果不存在则会创建，然后调用 Watchers#watch 方法将
   * 延时任务记录到 Watchers 对象的同步队列中
   */
  private def watchForOperation(key: Any, operation: T) {
    inReadLock(removeWatchersLock) {
      val watcher = watchersForKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers) {
    inWriteLock(removeWatchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (watchersForKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        watchersForKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown() {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
  }

  /**
   * A linked list of watched delayed operations based on some key
   */
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    def watch(t: T) {
      operations.add(t)
    }

    // traverse the list and try to complete some watched elements

    def tryCompleteWatched(): Int = {
      var completed = 0
      /**
        * 遍历处理当前 Watchers 对象中的延时任务
        */
      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()

        /**
          * 如果对应的延时任务已经执行完成，则从 Watchers 中移除
          */
        if (curr.isCompleted) {
          // another thread has completed this operation, just remove it
          iter.remove()

          /**
            * 尝试执行延时任务
            */
        } else if (curr.maybeTryComplete()) {
          iter.remove()
          completed += 1
        }
      }

      /**
        * 如果 key 对应的 Watchers 已空，则将 key 从 watchersForKey 中移除，防止内存泄露
        */
      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    def cancel(): List[T] = {
      val iter = operations.iterator()
      val cancelled = new ListBuffer[T]()
      while (iter.hasNext) {
        val curr = iter.next()
        curr.cancel()
        iter.remove()
        cancelled += curr
      }
      cancelled.toList
    }

    // traverse the list and purge elements that are already completed by others
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long) {
    /**
      * 尝试推进时间轮指针
      */
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    /**
      * 如果当前炼狱中的已完成任务数超过给定阈值 purgeInterval，则尝试清理
      */
    if (estimatedTotalOperations.get - delayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(delayed)

      /**
        * 清理期间会获取并处理所有在册的 Watchers 对象，
        * 通过调用每个 Watchers 对象的 Watchers#purgeCompleted 方法，
        * 对 Watchers 中已经执行完成的延时任务对象进行清理，如果某个 key 的 Watchers 对象
        * 在被清理之后不再包含任何等待执行的延时任务，则会调用
        * DelayedOperationPurgatory#removeKeyIfEmpty 方法将对应的 key
        * 从 DelayedOperationPurgatory#watchersForKey 字段中一并移除，防止内存泄露。
        */
      debug("Begin purging watch lists")
      /**
        * 遍历各个 Watcher 集合，执行清理操作
        */
      val purged = allWatchers.map(_.purgeCompleted()).sum
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
    * 它继承自 ShutdownableThread 抽象类，所以我们可以知道它本质上是一个线程类，
    * 在构造 DelayedOperationPurgatory 对象时如果设置 reaperEnabled=true 则会启动该线程
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d-%s".format(brokerId, purgatoryName),
    false) {
    /**
      * 调用 DelayedOperationPurgatory#advanceClock 方法在后台推动时间轮指针，
      * 并定期清理当前 DelayedOperationPurgatory 中记录的所有已执行完成的延时任务
      */
    override def doWork() {
      advanceClock(200L)
    }
  }
}
