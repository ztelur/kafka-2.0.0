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

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.cluster.{BrokerEndPoint, Partition, Replica}
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log.{Log, LogAppendInfo, LogConfig, LogManager}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.QuotaFactory.{QuotaManagers, UnboundedQuota}
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors.{KAFKA_STORAGE_ERROR, UNKNOWN_TOPIC_OR_PARTITION}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.DescribeLogDirsResponse.{LogDirInfo, ReplicaInfo}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection._

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

case class LogDeleteRecordsResult(requestedOffset: Long, lowWatermark: Long, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/*
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param hw high watermark of the local replica
 * @param readSize amount of data that was read from the log i.e. size of the fetch
 * @param isReadFromLogEnd true if the request read up to the log end offset snapshot
 *                         when the read was initiated, false otherwise
 * @param error Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         highWatermark: Long,
                         leaderLogStartOffset: Long,
                         leaderLogEndOffset: Long,
                         followerLogStartOffset: Long,
                         fetchTimeMs: Long,
                         readSize: Int,
                         lastStableOffset: Option[Long],
                         exception: Option[Throwable] = None) {

  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  def withEmptyFetchInfo: LogReadResult =
    copy(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY))

  override def toString =
    s"Fetch Data: [$info], HW: [$highWatermark], leaderLogStartOffset: [$leaderLogStartOffset], leaderLogEndOffset: [$leaderLogEndOffset], " +
    s"followerLogStartOffset: [$followerLogStartOffset], fetchTimeMs: [$fetchTimeMs], readSize: [$readSize], error: [$error]"

}

case class FetchPartitionData(error: Errors = Errors.NONE,
                              highWatermark: Long,
                              logStartOffset: Long,
                              records: Records,
                              lastStableOffset: Option[Long],
                              abortedTransactions: Option[List[AbortedTransaction]])

object LogReadResult {
  val UnknownLogReadResult = LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                                           highWatermark = -1L,
                                           leaderLogStartOffset = -1L,
                                           leaderLogEndOffset = -1L,
                                           followerLogStartOffset = -1L,
                                           fetchTimeMs = -1L,
                                           readSize = -1,
                                           lastStableOffset = None)
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L
  val OfflinePartition = new Partition("", -1, null, null, isOffline = true)
}

class ReplicaManager(val config: KafkaConfig, // // 相关配置对象
                     metrics: Metrics,
                     time: Time, // // 时间戳工具
                     val zkClient: KafkaZkClient, // ZK 工具类
                     scheduler: Scheduler,  // 定时任务调度器
                     val logManager: LogManager, // 用于对分区日志数据执行读写操作
                     val isShuttingDown: AtomicBoolean, // 标记 kafka 服务是否正在执行关闭操作
                     quotaManagers: QuotaManagers,
                     val brokerTopicStats: BrokerTopicStats,
                     val metadataCache: MetadataCache,
                     logDirFailureChannel: LogDirFailureChannel,
                     val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce], // 管理 DelayedFetch 延时任务的炼狱
                     val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                     val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords],
                     threadNamePrefix: Option[String]) extends Logging with KafkaMetricsGroup {

  def this(config: KafkaConfig,
           metrics: Metrics,
           time: Time,
           zkClient: KafkaZkClient,
           scheduler: Scheduler,
           logManager: LogManager,
           isShuttingDown: AtomicBoolean,
           quotaManagers: QuotaManagers,
           brokerTopicStats: BrokerTopicStats,
           metadataCache: MetadataCache,
           logDirFailureChannel: LogDirFailureChannel,
           threadNamePrefix: Option[String] = None) {
    this(config, metrics, time, zkClient, scheduler, logManager, isShuttingDown,
      quotaManagers, brokerTopicStats, metadataCache, logDirFailureChannel,
      DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", brokerId = config.brokerId,
        purgeInterval = config.producerPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", brokerId = config.brokerId,
        purgeInterval = config.fetchPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedDeleteRecords](
        purgatoryName = "DeleteRecords", brokerId = config.brokerId,
        purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests),
      threadNamePrefix)
  }

  /* epoch of the controller that last changed the leader */
  /**
    * 记录 kafka controller 的年代信息，当重新选择 controller leader 时会递增该字段，
    * 用于校验来自 controller 的请求的年代信息，防止处理来自老的 controller 的请求
    */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  /**
    * 本地 broker 的 ID
    */
  private val localBrokerId = config.brokerId
  /**
    * 记录当前 broker 管理的所有分区信息，如果不存在则创建
    */
  private val allPartitions = new Pool[TopicPartition, Partition](valueFactory = Some(tp =>
    new Partition(tp.topic, tp.partition, time, this)))
  private val replicaStateChangeLock = new Object
  /**
    * 管理向 leader 副本发送 FetchRequest 请求的 ReplicaFetcherThread 线程
    */
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManagers.follower)
  val replicaAlterLogDirsManager = createReplicaAlterLogDirsManager(quotaManagers.alterLogDirs, brokerTopicStats)
  /**
    * 标记 highwatermark-checkpoint 定时任务是否已经启动
    */
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  /**
    * 记录每个 log 目录与对应 topic 分区 HW 值的映射关系
    */
  @volatile var highWatermarkCheckpoints = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap
  /**
    * 标记 highwatermark-checkpoint 定时任务是否已经启动
    */
  private var hwThreadInitialized = false
  this.logIdent = s"[ReplicaManager broker=$localBrokerId] "
  private val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)
  /**
    * 记录 ISR 集合发生变化的 topic 分区信息
    */
  private val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
  private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())

  private var logDirFailureHandler: LogDirFailureHandler = null

  private class LogDirFailureHandler(name: String, haltBrokerOnDirFailure: Boolean) extends ShutdownableThread(name) {
    override def doWork() {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      if (haltBrokerOnDirFailure) {
        fatal(s"Halting broker because dir $newOfflineLogDir is offline")
        Exit.halt(1)
      }
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  val leaderCount = newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def value = leaderPartitionsIterator.size
    }
  )
  val partitionCount = newGauge(
    "PartitionCount",
    new Gauge[Int] {
      def value = allPartitions.size
    }
  )
  val offlineReplicaCount = newGauge(
    "OfflineReplicaCount",
    new Gauge[Int] {
      def value = offlinePartitionsIterator.size
    }
  )
  val underReplicatedPartitions = newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def value = underReplicatedPartitionCount
    }
  )
  val underMinIsrPartitionCount = newGauge(
    "UnderMinIsrPartitionCount",
    new Gauge[Int] {
      def value = leaderPartitionsIterator.count(_.isUnderMinIsr)
    }
  )

  val isrExpandRate = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS)
  val isrShrinkRate = newMeter("IsrShrinksPerSec", "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate = newMeter("FailedIsrUpdatesPerSec", "failedUpdates", TimeUnit.SECONDS)

  def underReplicatedPartitionCount: Int = leaderPartitionsIterator.count(_.isUnderReplicated)

  /**
    *
    * @return
    */
  def startHighWaterMarksCheckPointThread() = {
    if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))

    /**
      * 任务周期性将当前 broker 节点管理的每个 topic 分区的 HW 值更新到对应 log 目录下的 replication-offset-checkpoint 文件中
      */
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks _, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  def recordIsrChange(topicPartition: TopicPartition) {
    isrChangeSet synchronized {
      isrChangeSet += topicPartition
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }
  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  /**
    * 将 ISR 集合发生变化的 topic 副本信息更新到 ZK 相应节点下，Kafka 集群控制器基于 ZK 的 Watcher 机制监听相应节点，
    * 并在节点内容发生变化时向所有可用的 broker 节点发送 UpdateMetadataRequest 请求，以更新相应 broker 节点本地管理的整个集群中所有分区的状态信息
    */
  def maybePropagateIsrChanges() {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized {
      /**
        * 定期将 ISR 集合发生变化的分区记录到 ZK，kafka controller 对相应 ZK 路径添加了 Watcher，
        * 当 Watcher 被触发后会向所有可用的 broker 节点发送 UpdateMetadataRequest 请求，以更新 broker 节点缓存的所有分区状态信息
        */
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now ||

          /**
            * 最后一次有 ISR 集合发生变化的时间距离现在已经超过 5 秒
            * 上次写入 ZK 的时间距离现在已经超过 1 分钟
            */
          lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) {
        /**
          * 将 ISR 集合发生变更的 topic 分区信息记录到 ZK
          */
        zkClient.propagateIsrChanges(isrChangeSet)
        isrChangeSet.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }

  // When ReplicaAlterDirThread finishes replacing a current replica with a future replica, it will
  // remove the partition from the partition state map. But it will not close itself even if the
  // partition state map is empty. Thus we need to call shutdownIdleReplicaAlterDirThread() periodically
  // to shutdown idle ReplicaAlterDirThread
  def shutdownIdleReplicaAlterLogDirsThread(): Unit = {
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
  }

  def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

  /**
   * Try to complete some delayed produce requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for acks = -1)
   * 2. A follower replica's fetch operation is received (for acks > 1)
   */
  def tryCompleteDelayedProduce(key: DelayedOperationKey) {
    val completed = delayedProducePurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d producer requests.".format(key.keyLabel, completed))
  }

  /**
   * Try to complete some delayed fetch requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for regular fetch)
   * 2. A new message set is appended to the local log (for follower fetch)
   */
  def tryCompleteDelayedFetch(key: DelayedOperationKey) {
    val completed = delayedFetchPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d fetch requests.".format(key.keyLabel, completed))
  }

  /**
   * Try to complete some delayed DeleteRecordsRequest with the request key;
   * this needs to be triggered when the partition low watermark has changed
   */
  def tryCompleteDelayedDeleteRecords(key: DelayedOperationKey) {
    val completed = delayedDeleteRecordsPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d DeleteRecordsRequest.".format(key.keyLabel, completed))
  }

  /**
    * 启动 ReplicaManager 管理的定时任务
    */
  def startup() {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    /**
      * 定时检测当前 broker 节点管理的每个分区是否需要缩减 ISR 集合，并执行缩减操作
      */
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)

    /**
      * 定时将 ISR 集合发生变化的 topic 分区记录到 ZK
      */
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _, period = 2500L, unit = TimeUnit.MILLISECONDS)
    scheduler.schedule("shutdown-idle-replica-alter-log-dirs-thread", shutdownIdleReplicaAlterLogDirsThread _, period = 10000L, unit = TimeUnit.MILLISECONDS)

    // If inter-broker protocol (IBP) < 1.0, the controller will send LeaderAndIsrRequest V0 which does not include isNew field.
    // In this case, the broker receiving the request cannot determine whether it is safe to create a partition if a log directory has failed.
    // Thus, we choose to halt the broker on any log diretory failure if IBP < 1.0
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
  }

  def stopReplica(topicPartition: TopicPartition, deletePartition: Boolean)  = {
    stateChangeLogger.trace(s"Handling stop replica (delete=$deletePartition) for partition $topicPartition")

    /**
      * 如果 deletePartition = true，则删除分区对应的副本及其日志和索引文件
      */
    if (deletePartition) {
      /**
        * 从本地移除指定的 topic 分区
        */
      val removedPartition = allPartitions.remove(topicPartition)
      if (removedPartition eq ReplicaManager.OfflinePartition) {
        allPartitions.put(topicPartition, ReplicaManager.OfflinePartition)
        throw new KafkaStorageException(s"Partition $topicPartition is on an offline disk")
      }

      if (removedPartition != null) {
        val topicHasPartitions = allPartitions.values.exists(partition => topicPartition.topic == partition.topic)
        if (!topicHasPartitions)
          brokerTopicStats.removeMetrics(topicPartition.topic)
        // this will delete the local log. This call may throw exception if the log is on offline directory
        /**
          * 删除分区的日志和索引文件，并清空本地缓存的相关信息
          */
        removedPartition.delete()
      } else {
        stateChangeLogger.trace(s"Ignoring stop replica (delete=$deletePartition) for partition $topicPartition as replica doesn't exist on broker")
      }

      // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
      // This could happen when topic is being deleted while broker is down and recovers.
      /**
        * 本地未缓存对应的分区（一般发生在对应的 topic 已经被删除，但是期间 broker 宕机了），直接尝试对应的删除日志和索引文件
        */
      if (logManager.getLog(topicPartition).isDefined)
        logManager.asyncDelete(topicPartition)
      if (logManager.getLog(topicPartition, isFuture = true).isDefined)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
    stateChangeLogger.trace(s"Finished handling stop replica (delete=$deletePartition) for partition $topicPartition")
  }

  /**
    * stopReplicas
    * 当 broker 节点收到来自 kafka controller 的 StopReplicaRequest 请求时，会关闭指定的副本，
    * 包括停止副本的数据同步 fetcher 线程，以及依据参数决定是否删除副本对应的 Log 对象和文件，并清空本地缓存的相关信息
    * @param stopReplicaRequest
    * @return
    */
  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicPartition, Errors], Errors) = {
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[TopicPartition, Errors]

      /**
        * 校验 controller 的年代信息，避免处理来自已经过期的 controller 的请求
        */
      if(stopReplicaRequest.controllerEpoch() < controllerEpoch) {
        stateChangeLogger.warn("Received stop replica request from an old controller epoch " +
          s"${stopReplicaRequest.controllerEpoch}. Latest known controller epoch is $controllerEpoch")
        (responseMap, Errors.STALE_CONTROLLER_EPOCH)
      } else {
        val partitions = stopReplicaRequest.partitions.asScala

        /**
          * 更新本地记录的 kafka controller 的年代信息
          */
        controllerEpoch = stopReplicaRequest.controllerEpoch
        // First stop fetchers for all partitions, then stop the corresponding replicas
        /**
          * 停止对指定分区的数据同步 fetcher 线程
          */
        replicaFetcherManager.removeFetcherForPartitions(partitions)
        replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)
        for (topicPartition <- partitions){
          try {
            /**
              *  关闭指定分区的副本
              */
            stopReplica(topicPartition, stopReplicaRequest.deletePartitions)
            responseMap.put(topicPartition, Errors.NONE)
          } catch {
            case e: KafkaStorageException =>
              stateChangeLogger.error(s"Ignoring stop replica (delete=${stopReplicaRequest.deletePartitions}) for " +
                s"partition $topicPartition due to storage exception", e)
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
          }
        }
        (responseMap, Errors.NONE)
      }
    }
  }

  def getOrCreatePartition(topicPartition: TopicPartition): Partition =
    allPartitions.getAndMaybePut(topicPartition)

  def getPartition(topicPartition: TopicPartition): Option[Partition] =
    Option(allPartitions.get(topicPartition))

  def nonOfflinePartition(topicPartition: TopicPartition): Option[Partition] =
    getPartition(topicPartition).filter(_ ne ReplicaManager.OfflinePartition)

  private def nonOfflinePartitionsIterator: Iterator[Partition] =
    allPartitions.values.iterator.filter(_ ne ReplicaManager.OfflinePartition)

  private def offlinePartitionsIterator: Iterator[Partition] =
    allPartitions.values.iterator.filter(_ eq ReplicaManager.OfflinePartition)

  def getReplicaOrException(topicPartition: TopicPartition, brokerId: Int): Replica = {
    getPartition(topicPartition) match {
      case Some(partition) =>
        if (partition eq ReplicaManager.OfflinePartition)
          throw new KafkaStorageException(s"Replica $brokerId is in an offline log directory for partition $topicPartition")
        else
          partition.getReplica(brokerId).getOrElse(
            throw new ReplicaNotAvailableException(s"Replica $brokerId is not available for partition $topicPartition"))
      case None =>
        throw new ReplicaNotAvailableException(s"Replica $brokerId is not available for partition $topicPartition")
    }
  }

  def getReplicaOrException(topicPartition: TopicPartition): Replica = getReplicaOrException(topicPartition, localBrokerId)

  def getLeaderReplicaIfLocal(topicPartition: TopicPartition): Replica =  {
    val (_, replica) = getPartitionAndLeaderReplicaIfLocal(topicPartition)
    replica
  }

  def getPartitionAndLeaderReplicaIfLocal(topicPartition: TopicPartition): (Partition, Replica) =  {
    val partitionOpt = getPartition(topicPartition)
    partitionOpt match {
      case None if metadataCache.contains(topicPartition) =>
        // The topic exists, but this broker is no longer a replica of it, so we return NOT_LEADER which
        // forces clients to refresh metadata to find the new location. This can happen, for example,
        // during a partition reassignment if a produce request from the client is sent to a broker after
        // the local replica has been deleted.
        throw new NotLeaderForPartitionException(s"Broker $localBrokerId is not a replica of $topicPartition")

      case None =>
        throw new UnknownTopicOrPartitionException(s"Partition $topicPartition doesn't exist")

      case Some(partition) =>
        if (partition eq ReplicaManager.OfflinePartition)
          throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory on broker $localBrokerId")
        else partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => (partition, leaderReplica)
          case None =>
            throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
        }
    }
  }

  def getReplica(topicPartition: TopicPartition, replicaId: Int): Option[Replica] =
    nonOfflinePartition(topicPartition).flatMap(_.getReplica(replicaId))

  def getReplica(tp: TopicPartition): Option[Replica] = getReplica(tp, localBrokerId)

  def getLogDir(topicPartition: TopicPartition): Option[String] = {
    getReplica(topicPartition).flatMap(_.log) match {
      case Some(log) => Some(log.dir.getParent)
      case None => None
    }
  }

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied;
   * if the callback function itself is already synchronized on some object then pass this object to avoid deadlock.
   */
  /**
    * 用于处理 ProduceRequest 请求，将给定的日志数据追加到对应 topic 分区的 leader 副本中
    * @param timeout
    * @param requiredAcks
    * @param internalTopicsAllowed
    * @param isFromClient
    * @param entriesPerPartition
    * @param responseCallback
    * @param delayedProduceLock
    * @param recordConversionStatsCallback
    */
  def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    isFromClient: Boolean,
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                    delayedProduceLock: Option[Lock] = None,
                    recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => ()) {
    /**
      * 如果 acks 参数合法
      */
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = time.milliseconds
      /**
        * 将消息追加到 Log 对象中
        */
      val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,
        isFromClient = isFromClient, entriesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(time.milliseconds - sTime))
      /**
        * 封装数据追加结果
        */
      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  result.info.lastOffset + 1, // required offset 下一次请求日志的 offset 值
                  new PartitionResponse(result.error, result.info.firstOffset.getOrElse(-1), result.info.logAppendTime, result.info.logStartOffset)) // response status
      }

      recordConversionStatsCallback(localProduceResults.mapValues(_.info.recordConversionStats))

      /**
        * 如果需要生成 DelayedProduce 延时任务
        * acks 参数为 -1，表示需要 ISR 集合中全部的 follower 副本确认追加的消息数据；
        * 请求添加的消息数据不为空；
        * 至少有一个 topic 分区的消息追加成功。
        */
      if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        // create delayed produce operation
        /**
          * 创建 DelayedProduce 延时任务对象，将回调响应函数封装到延时任务对象中
          */
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
        /**
          * 创建当前延时任务监听的一系列 key 对象，监听本次追加操作的所有 topic 分区
          */
        val producerRequestKeys = entriesPerPartition.keys.map(new TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable.
        /**
          * 尝试执行延时任务，如果还未到期则将任务交由炼狱管理
          */
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // we can respond immediately
        /**
          * 无需生成 DelayedProduce 延时任务，立即响应
          */
        val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
        responseCallback(produceResponseStatus)
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      /**
        * 对应的 acks 参数错误，构造 INVALID_REQUIRED_ACKS 响应
        */
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UnknownLogAppendInfo.firstOffset.getOrElse(-1), RecordBatch.NO_TIMESTAMP, LogAppendInfo.UnknownLogAppendInfo.logStartOffset)
      }
      // 回调响应
      responseCallback(responseStatus)
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long]): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // reject delete records operation on internal topics
      if (Topic.isInternal(topicPartition.topic)) {
        (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val (partition, replica) = getPartitionAndLeaderReplicaIfLocal(topicPartition)
          val convertedOffset =
            if (requestedOffset == DeleteRecordsRequest.HIGH_WATERMARK) {
              replica.highWatermark.messageOffset
            } else
              requestedOffset
          if (convertedOffset < 0)
            throw new OffsetOutOfRangeException(s"The offset $convertedOffset for partition $topicPartition is not valid")

          val lowWatermark = partition.deleteRecordsOnLeader(convertedOffset)
          (topicPartition, LogDeleteRecordsResult(convertedOffset, lowWatermark))
        } catch {
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: OffsetOutOfRangeException |
                   _: PolicyViolationException |
                   _: KafkaStorageException) =>
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
          case t: Throwable =>
            error("Error processing delete records operation on partition %s".format(topicPartition), t)
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
        }
      }
    }
  }

  // If there exists a topic partition that meets the following requirement,
  // we need to put a delayed DeleteRecordsRequest and wait for the delete records operation to complete
  //
  // 1. the delete records operation on this partition is successful
  // 2. low watermark of this partition is smaller than the specified offset
  private def delayedDeleteRecordsRequired(localDeleteRecordsResults: Map[TopicPartition, LogDeleteRecordsResult]): Boolean = {
    localDeleteRecordsResults.exists{ case (_, deleteRecordsResult) =>
      deleteRecordsResult.exception.isEmpty && deleteRecordsResult.lowWatermark < deleteRecordsResult.requestedOffset
    }
  }

  /*
   * For each pair of partition and log directory specified in the map, if the partition has already been created on
   * this broker, move its log files to the specified log directory. Otherwise, record the pair in the memory so that
   * the partition will be created in the specified log directory when broker receives LeaderAndIsrRequest for the partition later.
   *
   */
  def alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors] = {
    replicaStateChangeLock synchronized {
      partitionDirs.map { case (topicPartition, destinationDir) =>
        try {
          if (!logManager.isLogDirOnline(destinationDir))
            throw new KafkaStorageException(s"Log directory $destinationDir is offline")

          getPartition(topicPartition) match {
            case Some(partition) =>
              if (partition eq ReplicaManager.OfflinePartition)
                throw new KafkaStorageException(s"Partition $topicPartition is offline")

              // Stop current replica movement if the destinationDir is different from the existing destination log directory
              if (partition.futureReplicaDirChanged(destinationDir)) {
                replicaAlterLogDirsManager.removeFetcherForPartitions(Set(topicPartition))
                partition.removeFutureLocalReplica()
              }

            case None =>
          }

          // If the log for this partition has not been created yet:
          // 1) Record the destination log directory in the memory so that the partition will be created in this log directory
          //    when broker receives LeaderAndIsrRequest for this partition later.
          // 2) Respond with ReplicaNotAvailableException for this partition in the AlterReplicaLogDirsResponse
          logManager.maybeUpdatePreferredLogDir(topicPartition, destinationDir)
          // throw ReplicaNotAvailableException if replica does not exit for the given partition
          getReplicaOrException(topicPartition)

          // If the destinationLDir is different from the current log directory of the replica:
          // - If there is no offline log directory, create the future log in the destinationDir (if it does not exist) and
          //   start ReplicaAlterDirThread to move data of this partition from the current log to the future log
          // - Otherwise, return KafkaStorageException. We do not create the future log while there is offline log directory
          //   so that we can avoid creating future log for the same partition in multiple log directories.
          if (getPartition(topicPartition).get.maybeCreateFutureReplica(destinationDir)) {
            val futureReplica = getReplicaOrException(topicPartition, Request.FutureLocalReplicaId)
            logManager.abortAndPauseCleaning(topicPartition)
            replicaAlterLogDirsManager.addFetcherForPartitions(Map(topicPartition -> BrokerAndInitialOffset(
              BrokerEndPoint(config.brokerId, "localhost", -1), futureReplica.highWatermark.messageOffset)))
          }

          (topicPartition, Errors.NONE)
        } catch {
          case e@(_: LogDirNotFoundException |
                  _: ReplicaNotAvailableException |
                  _: KafkaStorageException) =>
            (topicPartition, Errors.forException(e))
          case t: Throwable =>
            error("Error while changing replica dir for partition %s".format(topicPartition), t)
            (topicPartition, Errors.forException(t))
        }
      }
    }
  }

  /*
   * Get the LogDirInfo for the specified list of partitions.
   *
   * Each LogDirInfo specifies the following information for a given log directory:
   * 1) Error of the log directory, e.g. whether the log is online or offline
   * 2) size and lag of current and future logs for each partition in the given log directory. Only logs of the queried partitions
   *    are included. There may be future logs (which will replace the current logs of the partition in the future) on the broker after KIP-113 is implemented.
   */
  def describeLogDirs(partitions: Set[TopicPartition]): Map[String, LogDirInfo] = {
    val logsByDir = logManager.allLogs.groupBy(log => log.dir.getParent)

    config.logDirs.toSet.map { logDir: String =>
      val absolutePath = new File(logDir).getAbsolutePath
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        logsByDir.get(absolutePath) match {
          case Some(logs) =>
            val replicaInfos = logs.filter { log =>
              partitions.contains(log.topicPartition)
            }.map { log =>
              log.topicPartition -> new ReplicaInfo(log.size, getLogEndOffsetLag(log.topicPartition, log.logEndOffset, log.isFuture), log.isFuture)
            }.toMap

            (absolutePath, new LogDirInfo(Errors.NONE, replicaInfos.asJava))
          case None =>
            (absolutePath, new LogDirInfo(Errors.NONE, Map.empty[TopicPartition, ReplicaInfo].asJava))
        }

      } catch {
        case _: KafkaStorageException =>
          (absolutePath, new LogDirInfo(Errors.KAFKA_STORAGE_ERROR, Map.empty[TopicPartition, ReplicaInfo].asJava))
        case t: Throwable =>
          error(s"Error while describing replica in dir $absolutePath", t)
          (absolutePath, new LogDirInfo(Errors.forException(t), Map.empty[TopicPartition, ReplicaInfo].asJava))
      }
    }.toMap
  }

  def getLogEndOffsetLag(topicPartition: TopicPartition, logEndOffset: Long, isFuture: Boolean): Long = {
    getReplica(topicPartition) match {
      case Some(replica) =>
        if (isFuture)
          replica.logEndOffset.messageOffset - logEndOffset
        else
          math.max(replica.highWatermark.messageOffset - logEndOffset, 0)
      case None =>
        // return -1L to indicate that the LEO lag is not available if the replica is not created or is offline
        DescribeLogDirsResponse.INVALID_OFFSET_LAG
    }
  }

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsResponse.PartitionResponse] => Unit) {
    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        DeleteRecordsPartitionStatus(
          result.requestedOffset, // requested offset
          new DeleteRecordsResponse.PartitionResponse(result.lowWatermark, result.error)) // response status
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      // create delayed delete records operation
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed delete records operation
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(new TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed delete records operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys)
    } else {
      // we can respond immediately
      val deleteRecordsResponseStatus = deleteRecordsStatus.mapValues(status => status.responseStatus)
      responseCallback(deleteRecordsResponseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  private def delayedProduceRequestRequired(requiredAcks: Short,
                                            entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                            localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    entriesPerPartition.nonEmpty &&
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  /**
    * 相应 leader 副本对应的 Log 对象中追加日志数据
    * @param internalTopicsAllowed
    * @param isFromClient
    * @param entriesPerPartition
    * @param requiredAcks
    * @return
    */
  private def appendToLocalLog(internalTopicsAllowed: Boolean, // 是否允许往内部 topic 追加消息
                               isFromClient: Boolean,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords], // 对应分区需要追加的消息数据
                               requiredAcks: Short // acks
                                 ): Map[TopicPartition, LogAppendResult] = {
    trace(s"Append [$entriesPerPartition] to local log")

    /**
      * 遍历处理每个 topic 分区及其待追加的消息数据
      */
    entriesPerPartition.map { case (topicPartition, records) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      /**
        * 如果追加的对象是内部 topic，依据参数 internalTopicsAllowed 决定是否追加
        */
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
      } else {
        try {
          /**
            * 获取 topic 分区对应的 Partition 对象
            */
          val (partition, _) = getPartitionAndLeaderReplicaIfLocal(topicPartition)
          /**
            * 往 leader 副本对应的 Log 对象中追加消息数据
            * 方法将消息数据追加到指定 topic 分区的 leader 副本中。
            */
          val info = partition.appendRecordsToLeader(records, isFromClient, requiredAcks)
          val numAppendedMessages = info.numMessages

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          trace(s"${records.sizeInBytes} written to log ${topicPartition} beginning at offset " +
            s"${info.firstOffset.getOrElse(-1)} and ending at offset ${info.lastOffset}")

          /**
            * 回每个分区写入的消息结果
            */
          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException |
                   _: InvalidTimestampException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case t: Throwable =>
            val logStartOffset = getPartition(topicPartition) match {
              case Some(partition) =>
                partition.logStartOffset
              case _ =>
                -1
            }
            brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
            brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
            error("Error processing append operation on partition %s".format(topicPartition), t)
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset), Some(t)))
        }
      }
    }
  }

  /**
   * Fetch messages from the leader replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied
    *
    * 用于处理来自消费者或 follower 副本读取消息数据的 FetchRequest 请求
    *
    *
    * 从本地副本读取指定位置和大小的消息数据；
    * 如果是来自 follower 副本的请求，则更新对应的 follower 副本的状态信息，并尝试扩张对应 topic 分区的 ISR 集合，同时尝试执行监听该分区的 DelayedProduce 延时任务；
    * 判定是否需要对请求方进行立即响应，如果需要则立即触发响应回调函数；
    * 否则，构造 DelayedFetch 延时任务，监听对应的 topic 分区对象，并交由炼狱管理。
    *
   */
  def fetchMessages(timeout: Long,
                    replicaId: Int,
                    fetchMinBytes: Int,
                    fetchMaxBytes: Int,
                    hardMaxBytesLimit: Boolean,
                    fetchInfos: Seq[(TopicPartition, PartitionData)],
                    quota: ReplicaQuota = UnboundedQuota,
                    responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit,
                    isolationLevel: IsolationLevel) {
    /**
      * 标记是否是来自 follower 的 fetch 请求
      */
    val isFromFollower = Request.isValidBrokerId(replicaId)
    /**
      * 是否只读 leader 副本的消息，一般 debug 模式下可以读 follower 副本的数据
      */
    val fetchOnlyFromLeader = replicaId != Request.DebuggingConsumerId && replicaId != Request.FutureLocalReplicaId
    /**
      * 是否只读已完成提交的消息（即 HW 之前的消息），如果是来自消费者的请求则该参数是 true，如果是 follower 则该参数是 false
      */
    val fetchOnlyCommitted = !isFromFollower && replicaId != Request.FutureLocalReplicaId

    def readFromLog(): Seq[(TopicPartition, LogReadResult)] = {
      /**
        * 读取指定位置和大小的消息数据
        */
      val result = readFromLocalLog(
        replicaId = replicaId,
        fetchOnlyFromLeader = fetchOnlyFromLeader,
        readOnlyCommitted = fetchOnlyCommitted,
        fetchMaxBytes = fetchMaxBytes,
        hardMaxBytesLimit = hardMaxBytesLimit,
        readPartitionInfo = fetchInfos,
        quota = quota,
        isolationLevel = isolationLevel)

      /**
        * 如果当前是来自 follower 的同步消息数据请求，则更新 follower 副本的状态，
        * 并尝试扩张 ISR 集合，同时尝试触发监听对应 topic 分区的 DelayedProduce 延时任务
        */
      if (isFromFollower) updateFollowerLogReadResults(replicaId, result)
      else result
    }

    val logReadResults = readFromLog()

    // check if this fetch request can be satisfied right away
    val logReadResultValues = logReadResults.map { case (_, v) => v }
    val bytesReadable = logReadResultValues.map(_.info.records.sizeInBytes).sum
    val errorReadingData = logReadResultValues.foldLeft(false) ((errorIncurred, readResult) =>
      errorIncurred || (readResult.error != Errors.NONE))

    // respond immediately if 1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    if (timeout <= 0  // 请求希望立即响应
      || fetchInfos.isEmpty  // 请求不期望有响应数据
      || bytesReadable >= fetchMinBytes  // 已经有足够的数据可以响应
      || errorReadingData) { // 读取数据出现错误
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
          result.lastStableOffset, result.info.abortedTransactions)
      }

      /**
        * 立即响应
        */
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      /**
        * 构造 DelayedFetch 延时任务
        */
      val fetchPartitionStatus = logReadResults.map { case (topicPartition, result) =>
        val fetchInfo = fetchInfos.collectFirst {
          case (tp, v) if tp == topicPartition => v
        }.getOrElse(sys.error(s"Partition $topicPartition not found in fetchInfos"))
        (topicPartition, FetchPartitionStatus(result.info.fetchOffsetMetadata, fetchInfo))
      }
      val fetchMetadata = FetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit, fetchOnlyFromLeader,
        fetchOnlyCommitted, isFromFollower, replicaId, fetchPartitionStatus)
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, isolationLevel, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      /**
        * 构造延时任务关注的 key，即相应的 topic 分区对象
        */
      val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => new TopicPartitionOperationKey(tp) }

      // try to complete the request immediately, otherwise put it into the purgatory;
      // this is because while the delayed fetch operation is being created, new requests
      // may arrive and hence make this operation completable.
      /**
        * 交由炼狱管理
        */
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
    *
    * 从本地读取指定 topic 分区相应位置和大小的消息数据的功能，具体的消息数据读操作由 Log#read 方法实现
    *
   */
  def readFromLocalLog(replicaId: Int, // 请求的 follower 副本 ID
                       fetchOnlyFromLeader: Boolean, // 是否只读 leader 副本的消息，一般 debug 模式下可以读 follower 副本的数据
                       readOnlyCommitted: Boolean, // 是否只读已完成提交的消息（即 HW 之前的消息），如果是来自消费者的请求则该参数是 true，如果是 follower 则该参数是 false
                       fetchMaxBytes: Int, // 最大 fetch 字节数
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)], // 每个分区读取的起始 offset 和最大字节数
                       quota: ReplicaQuota,
                       isolationLevel: IsolationLevel): Seq[(TopicPartition, LogReadResult)] = {

    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      brokerTopicStats.topicStats(tp.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      try {
        trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
          s"remaining response limit $limitBytes" +
          (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        // decide whether to only fetch from leader
        /**
          * 获取待读取消息的副本对象，一般都是从本地副本读取（debug 模式除外）
          */
        val localReplica = if (fetchOnlyFromLeader)
          getLeaderReplicaIfLocal(tp)
        else
          getReplicaOrException(tp)

        val initialHighWatermark = localReplica.highWatermark.messageOffset //  HW
        val lastStableOffset = if (isolationLevel == IsolationLevel.READ_COMMITTED)
          Some(localReplica.lastStableOffset.messageOffset)
        else
          None

        // decide whether to only fetch committed data (i.e. messages below high watermark)
        /**
          * 计算读取消息的 offset 上界，如果是来自消费者的请求，则上界为 HW，如果是来自 follower 的请求，则上界为 LEO
          */
        val maxOffsetOpt = if (readOnlyCommitted)
          Some(lastStableOffset.getOrElse(initialHighWatermark))
        else
          None

        /* Read the LogOffsetMetadata prior to performing the read from the log.
         * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
         * Using the log end offset after performing the read can lead to a race condition
         * where data gets appended to the log immediately after the replica has consumed from it
         * This can cause a replica to always be out of sync.
         */
        val initialLogEndOffset = localReplica.logEndOffset.messageOffset // // LEO
        val initialLogStartOffset = localReplica.logStartOffset
        val fetchTimeMs = time.milliseconds
        val logReadInfo = localReplica.log match {
          case Some(log) =>
            val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)
            /**
              * 从 Log 中读取消息数据
              */
            // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
            val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage, isolationLevel)

            // If the partition is being throttled, simply return an empty set.
            /**
              * 限流检测
              */
            if (shouldLeaderThrottle(quota, tp, replicaId))
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
            // progress in such cases and don't need to report a `RecordTooLargeException`
            else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete)
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            else fetch

          case None =>

            /**
              * 对应副本的 Log 对象不存在
              */
            error(s"Leader for partition $tp does not have a local log")
            FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
        }

        /**
          * 封装结果返回
          */
        LogReadResult(info = logReadInfo,
                      highWatermark = initialHighWatermark,
                      leaderLogStartOffset = initialLogStartOffset,
                      leaderLogEndOffset = initialLogEndOffset,
                      followerLogStartOffset = followerLogStartOffset,
                      fetchTimeMs = fetchTimeMs,
                      readSize = partitionFetchSize,
                      lastStableOffset = lastStableOffset,
                      exception = None)
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderForPartitionException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: OffsetOutOfRangeException) =>
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                        highWatermark = -1L,
                        leaderLogStartOffset = -1L,
                        leaderLogEndOffset = -1L,
                        followerLogStartOffset = -1L,
                        fetchTimeMs = -1L,
                        readSize = partitionFetchSize,
                        lastStableOffset = None,
                        exception = Some(e))
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()
          error(s"Error processing fetch operation on partition $tp, offset $offset", e)
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                        highWatermark = -1L,
                        leaderLogStartOffset = -1L,
                        leaderLogEndOffset = -1L,
                        followerLogStartOffset = -1L,
                        fetchTimeMs = -1L,
                        readSize = partitionFetchSize,
                        lastStableOffset = None,
                        exception = Some(e))
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    var minOneMessage = !hardMaxBytesLimit

    /**
      * 遍历读取每个 topic 分区的消息数据
      */
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize = readResult.info.records.sizeInBytes
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (recordBatchSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      result += (tp -> readResult)
    }
    result
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, topicPartition: TopicPartition, replicaId: Int): Boolean = {
    val isReplicaInSync = nonOfflinePartition(topicPartition).exists { partition =>
      partition.getReplica(replicaId).exists(partition.inSyncReplicas.contains)
    }
    quota.isThrottled(topicPartition) && quota.isQuotaExceeded && !isReplicaInSync
  }

  def getLogConfig(topicPartition: TopicPartition): Option[LogConfig] = getReplica(topicPartition).flatMap(_.log.map(_.config))

  def getMagic(topicPartition: TopicPartition): Option[Byte] = getLogConfig(topicPartition).map(_.messageFormatVersion.recordVersion.value)

  /**
    *
    *
    * kafka Controller 在成为 leader 角色时会在相应 ZK 路径上注册 Watcher 监听器，当监听到有数据变化时，会构建 UpdateMetadataRequest 请求对象发送给所有可用的 broker 节点
    * 处理 UpdateMetadataRequest 请求
    * @param correlationId
    * @param updateMetadataRequest
    * @return
    *
    *         用于处理 UpdateMetadataRequest 请求，该方法首先会校验请求中 kafka controller 的年代信息，以避免处理来自已经过期的
    *         kafka controller 的请求，对于合法的请求则会调用 MetadataCache#updateCache 方法更新本地缓存的整个集群的 topic 分区状态信息
    */
  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) : Seq[TopicPartition] =  {
    replicaStateChangeLock synchronized {
      /**
        * 校验 controller 的年代信息，避免处理来自已经过期的 controller 的请求
        */
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) {
        val stateControllerEpochErrorMessage = s"Received update metadata request with correlation id $correlationId " +
          s"from an old controller ${updateMetadataRequest.controllerId} with epoch ${updateMetadataRequest.controllerEpoch}. " +
          s"Latest known controller epoch is $controllerEpoch"
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateChangeLogger.messageWithPrefix(stateControllerEpochErrorMessage))
      } else {
        /**
          * 更新所有分区的状态信息，并返回需要被移除的分区集合
          */
        val deletedPartitions = metadataCache.updateCache(correlationId, updateMetadataRequest)

        /**
          * 更新本地缓存的 controller 年代信息
          */
        controllerEpoch = updateMetadataRequest.controllerEpoch
        deletedPartitions
      }
    }
  }

  /**
    * 用于处理来自 kafka controller 的 LeaderAndIsrRequest 请求，
    * 指导位于当前 broker 节点上的相应分区副本的角色切换工作
    * @param correlationId
    * @param leaderAndIsrRequest
    * @param onLeadershipChange
    * @return
    */
  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndIsrRequest: LeaderAndIsrRequest,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): LeaderAndIsrResponse = {
    leaderAndIsrRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
      stateChangeLogger.trace(s"Received LeaderAndIsr request $stateInfo " +
        s"correlation id $correlationId from controller ${leaderAndIsrRequest.controllerId} " +
        s"epoch ${leaderAndIsrRequest.controllerEpoch} for partition $topicPartition")
    }
    replicaStateChangeLock synchronized {
      /**
        * 校验 controller 的年代信息，避免处理来自已经过期的 controller 的请求
        */
      if (leaderAndIsrRequest.controllerEpoch < controllerEpoch) {
        stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller ${leaderAndIsrRequest.controllerId} with " +
          s"correlation id $correlationId since its controller epoch ${leaderAndIsrRequest.controllerEpoch} is old. " +
          s"Latest known controller epoch is $controllerEpoch")
        leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_CONTROLLER_EPOCH.exception)
      } else {
        /**
          * 用于记录每个分区角色切换操作的状态码
          */
        val responseMap = new mutable.HashMap[TopicPartition, Errors]
        val controllerId = leaderAndIsrRequest.controllerId

        /**
          * 更新本地缓存的 kafka controller 的年代信息
          */
        controllerEpoch = leaderAndIsrRequest.controllerEpoch

        // First check partition's leader epoch
        /**
          * 校验请求的 leader 副本的年代信息，以及是否由当前 broker 节点管理，将满足条件的分区信息记录到 partitionState 集合中
          */
        val partitionState = new mutable.HashMap[Partition, LeaderAndIsrRequest.PartitionState]()
        val newPartitions = leaderAndIsrRequest.partitionStates.asScala.keys.filter(topicPartition => getPartition(topicPartition).isEmpty)

        leaderAndIsrRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>

          /**
            * 获取/创建指定 topic 分区的 Partition 对象
            */
          val partition = getOrCreatePartition(topicPartition)
          /**
            * 获取 leader 副本的年代信息
            */
          val partitionLeaderEpoch = partition.getLeaderEpoch
          if (partition eq ReplicaManager.OfflinePartition) {
            stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
              s"controller $controllerId with correlation id $correlationId " +
              s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
              "partition is in an offline log directory")
            responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)

            /**
              * 校验 leader 副本的年代信息，需要保证请求中的 leader 副本的年代信息大于本地缓存的 topic 分区 leader 副本的年代信息
              */
          } else if (partitionLeaderEpoch < stateInfo.basePartitionState.leaderEpoch) {
            // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
            // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
            /**
              * 如果请求的分区副本位于当前 broker 节点上，记录到 partitionState 集合中
              */
            if(stateInfo.basePartitionState.replicas.contains(localBrokerId))
              partitionState.put(partition, stateInfo)
            else {
              /**
                * 请求的分区副本不在当前 broker 节点上，响应 UNKNOWN_TOPIC_OR_PARTITION 错误
                */
              stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
                s"correlation id $correlationId epoch $controllerEpoch for partition $topicPartition as itself is not " +
                s"in assigned replica list ${stateInfo.basePartitionState.replicas.asScala.mkString(",")}")
              responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            }
          } else {
            // Otherwise record the error code in response
            stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
              s"controller $controllerId with correlation id $correlationId " +
              s"epoch $controllerEpoch for partition $topicPartition since its associated " +
              s"leader epoch ${stateInfo.basePartitionState.leaderEpoch} is not higher than the current " +
              s"leader epoch $partitionLeaderEpoch")

            /**
              * 请求中的 leader 副本的年代信息小于等于本地记录的对应 topic 分区 leader 副本的年代信息，响应 STALE_CONTROLLER_EPOCH 错误
              */
            responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
          }
        }
        /**
          * 3. 将请求对象中的分区集合分割成 leader 和 follower 两类，并执行角色切换
          */
        val partitionsTobeLeader = partitionState.filter { case (_, stateInfo) =>
          stateInfo.basePartitionState.leader == localBrokerId
        }
        val partitionsToBeFollower = partitionState -- partitionsTobeLeader.keys
        /**
          * 将指定分区的副本切换成 leader 角色
          */
        val partitionsBecomeLeader = if (partitionsTobeLeader.nonEmpty)
          makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap)
        else
          Set.empty[Partition]
        /**
          * 将指定分区的副本切换成 follower 角色
          */
        val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
          makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap)
        else
          Set.empty[Partition]

        leaderAndIsrRequest.partitionStates.asScala.keys.foreach(topicPartition =>
          /*
           * If there is offline log directory, a Partition object may have been created by getOrCreatePartition()
           * before getOrCreateReplica() failed to create local replica due to KafkaStorageException.
           * In this case ReplicaManager.allPartitions will map this topic-partition to an empty Partition object.
           * we need to map this topic-partition to OfflinePartition instead.
           */
          if (getReplica(topicPartition).isEmpty && (allPartitions.get(topicPartition) ne ReplicaManager.OfflinePartition))
            allPartitions.put(topicPartition, ReplicaManager.OfflinePartition)
        )

        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions
        /**
          * 如果 highwatermark-checkpoint 定时任务尚未启动，则执行启动
          */
        if (!hwThreadInitialized) {
          startHighWaterMarksCheckPointThread()
          hwThreadInitialized = true
        }

        val newOnlineReplicas = newPartitions.flatMap(topicPartition => getReplica(topicPartition))
        // Add future replica to partition's map
        val futureReplicasAndInitialOffset = newOnlineReplicas.filter { replica =>
          logManager.getLog(replica.topicPartition, isFuture = true).isDefined
        }.map { replica =>
          replica.topicPartition -> BrokerAndInitialOffset(BrokerEndPoint(config.brokerId, "localhost", -1), replica.highWatermark.messageOffset)
        }.toMap
        futureReplicasAndInitialOffset.keys.foreach(tp => getPartition(tp).get.getOrCreateReplica(Request.FutureLocalReplicaId))

        // pause cleaning for partitions that are being moved and start ReplicaAlterDirThread to move replica from source dir to destination dir
        futureReplicasAndInitialOffset.keys.foreach(logManager.abortAndPauseCleaning)
        replicaAlterLogDirsManager.addFetcherForPartitions(futureReplicasAndInitialOffset)

        /**
          * 关闭空闲的 fetcher 线程
          */
        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()

        /**
          * 执行回调函数，完成 GroupCoordinator 的迁移操作
          */
        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)

        /**
          * 封装结果对象返回
          */
        new LeaderAndIsrResponse(Errors.NONE, responseMap.asJava)
      }
    }
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  /**
    *
    * @param controllerId
    * @param epoch
    * @param partitionState
    * @param correlationId
    * @param responseMap
    * @return
    */
  private def makeLeaders(controllerId: Int,
                          epoch: Int,
                          partitionState: Map[Partition, LeaderAndIsrRequest.PartitionState], // 记录需要切换成 leader 角色的分区副本信息
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Errors]): Set[Partition] = {
    /**
      * 初始化每个 topic 分区的错误码为 NONE
      */
    partitionState.keys.foreach { partition =>
      stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from " +
        s"controller $controllerId epoch $epoch starting the become-leader transition for " +
        s"partition ${partition.topicPartition}")
    }

    for (partition <- partitionState.keys)
      responseMap.put(partition.topicPartition, Errors.NONE)

    val partitionsToMakeLeaders = mutable.Set[Partition]()

    try {
      // First stop fetchers for all the partitions
      /**
        *  如果对应的副本当前是 follower 角色，需要要先停止这些副本的消息同步工作
        */
      replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(_.topicPartition))
      // Update the partition information to be the leader
      /**
        * 遍历处理 partitionState 集合，将其中记录的分区转换成 leader 角色
        */
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        try {
          /**
            * 调用 Partition#makeLeader 方法，将分区的本地副本切换成 leader 角色
            */
          if (partition.makeLeader(controllerId, partitionStateInfo, correlationId)) {
            partitionsToMakeLeaders += partition
            stateChangeLogger.trace(s"Stopped fetchers as part of become-leader request from " +
              s"controller $controllerId epoch $epoch with correlation id $correlationId for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionStateInfo.basePartitionState.controllerEpoch})")
          } else
            stateChangeLogger.info(s"Skipped the become-leader state change after marking its " +
              s"partition as leader with correlation id $correlationId from controller $controllerId epoch $epoch for " +
              s"partition ${partition.topicPartition} (last update controller epoch ${partitionStateInfo.basePartitionState.controllerEpoch}) " +
              s"since it is already the leader for the partition.")
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-leader state change with " +
              s"correlation id $correlationId from controller $controllerId epoch $epoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionStateInfo.basePartitionState.controllerEpoch}) since " +
              s"the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the leader for partition $partition in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

    } catch {
      case e: Throwable =>
        partitionState.keys.foreach { partition =>
          stateChangeLogger.error(s"Error while processing LeaderAndIsr request correlationId $correlationId received " +
            s"from controller $controllerId epoch $epoch for partition ${partition.topicPartition}", e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.keys.foreach { partition =>
      stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
        s"epoch $epoch for the become-leader transition for partition ${partition.topicPartition}")
    }

    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. Clear the produce and fetch requests in the purgatory
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   *
   *
   * 切换副本为 follower 角色的过程相对要复杂一些，整体执行流程可以概括为：

检测对应新的 leader 副本所在 broker 节点是否可用，如果不可用则无需执行切换操作，否则调用 Partition#makeFollower 方法执行副本角色切换；
停止待切换副本的数据同步 fetcher 线程；
由于 leader 副本发生变化，新旧 leader 在 [HW, LEO] 之间的数据可能不一致，所以需要将当前副本截断到 HW 位置，以保证数据一致性；
尝试完成监听对应分区的 DelayedProduce 和 DelayedFetch 延时任务；
为新的 follower 副本集合创建并启动对应的数据同步 fetcher 线程（如果已存在，则复用）

   */
  private def makeFollowers(controllerId: Int,
                            epoch: Int,
                            partitionStates: Map[Partition, LeaderAndIsrRequest.PartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Errors]) : Set[Partition] = {

    partitionStates.foreach { case (partition, partitionState) =>
      stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
        s"epoch $epoch starting the become-follower transition for partition ${partition.topicPartition} with leader " +
        s"${partitionState.basePartitionState.leader}")
    }

    /**
      * 初始化每个 topic 分区的错误码为 NONE
      */
    for (partition <- partitionStates.keys)
      responseMap.put(partition.topicPartition, Errors.NONE)

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()

    try {

      // TODO: Delete leaders from LeaderAndIsrRequest
      partitionStates.foreach { case (partition, partitionStateInfo) =>

        /**
          * 检测 leader 副本所在的 broker 是否可用
          */
        val newLeaderBrokerId = partitionStateInfo.basePartitionState.leader
        try {
          metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
            // Only change partition state when the leader is available
            /**
              * 仅对 leader 副本所在 broker 节点可用的副本执行角色切换
              */
            case Some(_) =>
              if (partition.makeFollower(controllerId, partitionStateInfo, correlationId))
                partitionsToMakeFollower += partition
              else
                stateChangeLogger.info(s"Skipped the become-follower state change after marking its partition as " +
                  s"follower with correlation id $correlationId from controller $controllerId epoch $epoch " +
                  s"for partition ${partition.topicPartition} (last update " +
                  s"controller epoch ${partitionStateInfo.basePartitionState.controllerEpoch}) " +
                  s"since the new leader $newLeaderBrokerId is the same as the old leader")

            /**
              *  对应 leader 副本所在的 broker 节点失效
              */
            case None =>
              // The leader broker should always be present in the metadata cache.
              // If not, we should record the error message and abort the transition process for this partition
              stateChangeLogger.error(s"Received LeaderAndIsrRequest with correlation id $correlationId from " +
                s"controller $controllerId epoch $epoch for partition ${partition.topicPartition} " +
                s"(last update controller epoch ${partitionStateInfo.basePartitionState.controllerEpoch}) " +
                s"but cannot become follower since the new leader $newLeaderBrokerId is unavailable.")
              // Create the local replica even if the leader is unavailable. This is required to ensure that we include
              // the partition's high watermark in the checkpoint file (see KAFKA-1647)
              /**
                * 即使 leader 副本所在的 broker 不可用，也要创建本地副本对象，主要是为了在 checkpoint 文件中记录此分区的 HW 值
                */
              partition.getOrCreateReplica(isNew = partitionStateInfo.isNew)
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-follower state change with correlation id $correlationId from " +
              s"controller $controllerId epoch $epoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionStateInfo.basePartitionState.controllerEpoch}) with leader " +
              s"$newLeaderBrokerId since the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the follower for partition $partition with leader " +
              s"$newLeaderBrokerId in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

      /**
        * 停止与旧的 leader 副本同步的 fetcher 线程
        */
      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(s"Stopped fetchers as part of become-follower request from controller $controllerId " +
          s"epoch $epoch with correlation id $correlationId for partition ${partition.topicPartition} with leader " +
          s"${partitionStates(partition).basePartitionState.leader}")
      }

      /**
        * 尝试完成监听对应分区的 DelayedProduce 和 DelayedFetch 延时任务
        */
      partitionsToMakeFollower.foreach { partition =>
        val topicPartitionOperationKey = new TopicPartitionOperationKey(partition.topicPartition)
        tryCompleteDelayedProduce(topicPartitionOperationKey)
        tryCompleteDelayedFetch(topicPartitionOperationKey)
      }

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(s"Truncated logs and checkpointed recovery boundaries for partition " +
          s"${partition.topicPartition} as part of become-follower request with correlation id $correlationId from " +
          s"controller $controllerId epoch $epoch with leader ${partitionStates(partition).basePartitionState.leader}")
      }

      /**
        * 检测 ReplicaManager 的运行状态
        */
      if (isShuttingDown.get()) {
        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(s"Skipped the adding-fetcher step of the become-follower state " +
            s"change with correlation id $correlationId from controller $controllerId epoch $epoch for " +
            s"partition ${partition.topicPartition} with leader ${partitionStates(partition).basePartitionState.leader} " +
            "since it is shutting down")
        }
      }
      else {
        /**
          * 重新启用与新 leader 副本同步的 fetcher 线程
          */
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
          partition.topicPartition -> BrokerAndInitialOffset(
            metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get.brokerEndPoint(config.interBrokerListenerName),
            partition.getReplica().get.highWatermark.messageOffset)).toMap

        /**
          * 为需要同步的分区创建并启动同步线程，从指定的 offset 开始与 leader 副本进行同步
          */
        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)

        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(s"Started fetcher to new leader as part of become-follower " +
            s"request from controller $controllerId epoch $epoch with correlation id $correlationId for " +
            s"partition ${partition.topicPartition} with leader ${partitionStates(partition).basePartitionState.leader}")
        }
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"Error while processing LeaderAndIsr request with correlationId $correlationId " +
          s"received from controller $controllerId epoch $epoch", e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionStates.keys.foreach { partition =>
      stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
        s"epoch $epoch for the become-follower transition for partition ${partition.topicPartition} with leader " +
        s"${partitionStates(partition).basePartitionState.leader}")
    }

    partitionsToMakeFollower
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    nonOfflinePartitionsIterator.foreach(_.maybeShrinkIsr(config.replicaLagTimeMaxMs))
  }

  /**
   * Update the follower's fetch state in the leader based on the last fetch request and update `readResult`,
   * if the follower replica is not recognized to be one of the assigned replicas. Do not update
   * `readResult` otherwise, so that log start/end offset and high watermark is consistent with
   * records in fetch response. Log start/end offset and high watermark may change not only due to
   * this fetch request, e.g., rolling new log segment and removing old log segment may move log
   * start offset further than the last offset in the fetched records. The followers will get the
   * updated leader's state in the next fetch response.
    *
    *
    * 更新指定 follower 副本的状态信息（包括 LEO 值、最近一次成功从 leader 拉取消息的时间戳等）；
    * 尝试扩张副本所属分区的 ISR 集合，因为 follower 的 LEO 值递增，可能已经符合加入 ISR 集合的条件；
    * 因为有新的消息被成功追加，尝试后移对应 leader 副本的 HW 值；
    * 尝试执行监听对应 topic 分区的 DelayedProduce 延时任务。
   */
  private def updateFollowerLogReadResults(replicaId: Int,
                                           readResults: Seq[(TopicPartition, LogReadResult)]): Seq[(TopicPartition, LogReadResult)] = {
    debug(s"Recording follower broker $replicaId log end offsets: $readResults")

    /**
      * 遍历处理对应 topic 分区的日志数据读取结果
      */
    readResults.map { case (topicPartition, readResult) =>
      var updatedReadResult = readResult
      nonOfflinePartition(topicPartition) match {
        case Some(partition) =>
          partition.getReplica(replicaId) match {
            case Some(replica) =>

              /**
                * 更新指定 follower 副本的状态，并尝试扩张对应分区的 ISR 集合，以及后移 leader 副本的 HW 值
                */
              partition.updateReplicaLogReadResult(replica, readResult)
            case None =>
              warn(s"Leader $localBrokerId failed to record follower $replicaId's position " +
                s"${readResult.info.fetchOffsetMetadata.messageOffset} since the replica is not recognized to be " +
                s"one of the assigned replicas ${partition.assignedReplicas.map(_.brokerId).mkString(",")} " +
                s"for partition $topicPartition. Empty records will be returned for this partition.")
              updatedReadResult = readResult.withEmptyFetchInfo
          }
        case None =>
          warn(s"While recording the replica LEO, the partition $topicPartition hasn't been created.")
      }
      topicPartition -> updatedReadResult
    }
  }

  private def leaderPartitionsIterator: Iterator[Partition] =
    nonOfflinePartitionsIterator.filter(_.leaderReplicaIfLocal.isDefined)

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
    nonOfflinePartition(topicPartition).flatMap(_.leaderReplicaIfLocal.map(_.logEndOffset.messageOffset))

  // Flushes the highwatermark value for all partitions to the highwatermark file
  /**
    * ReplicaManager 还定义了另外一个定时任务 highwatermark-checkpoint，该任务周期性将当前 broker 节点管理的每个
    * topic 分区的 HW 值更新到对应 log 目录下的 replication-offset-checkpoint 文件中
    */
  def checkpointHighWatermarks() {
    /**
      * 获取所有分区全部的本地副本 Replica 对象
      */
    val replicas = nonOfflinePartitionsIterator.flatMap { partition =>
      val replicasList: mutable.Set[Replica] = mutable.Set()
      partition.getReplica(localBrokerId).foreach(replicasList.add)
      partition.getReplica(Request.FutureLocalReplicaId).foreach(replicasList.add)
      replicasList
    }.filter(_.log.isDefined).toBuffer
    /**
      * 按照副本所在的 log 目录进行分组
      */
    val replicasByDir = replicas.groupBy(_.log.get.dir.getParent)

    /**
      * 遍历将位于相同 log 目录下的分区 HW 值，写入到对应的 replication-offset-checkpoint 文件中
      */
    for ((dir, reps) <- replicasByDir) {
      /**
        * 获取每个 topic 分区对应的 HW 值
        */
      val hwms = reps.map(r => r.topicPartition -> r.highWatermark.messageOffset).toMap
      try {
        /**
          * 更新对应 log 目录下的 replication-offset-checkpoint 文件
          */
        highWatermarkCheckpoints.get(dir).foreach(_.write(hwms))
      } catch {
        case e: KafkaStorageException =>
          error(s"Error while writing to highwatermark file in directory $dir", e)
      }
    }
  }

  // Used only by test
  def markPartitionOffline(tp: TopicPartition) {
    allPartitions.put(tp, ReplicaManager.OfflinePartition)
  }

  // logDir should be an absolute path
  // sendZkNotification is needed for unit test
  def handleLogDirFailure(dir: String, sendZkNotification: Boolean = true) {
    if (!logManager.isLogDirOnline(dir))
      return
    info(s"Stopping serving replicas in dir $dir")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = nonOfflinePartitionsIterator.filter { partition =>
        partition.getReplica(config.brokerId).exists { replica =>
          replica.log.isDefined && replica.log.get.dir.getParent == dir
        }
      }.map(_.topicPartition).toSet

      val partitionsWithOfflineFutureReplica = nonOfflinePartitionsIterator.filter { partition =>
        partition.getReplica(Request.FutureLocalReplicaId).exists { replica =>
          replica.log.isDefined && replica.log.get.dir.getParent == dir
        }
      }.toSet

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions)
      replicaAlterLogDirsManager.removeFetcherForPartitions(newOfflinePartitions ++ partitionsWithOfflineFutureReplica.map(_.topicPartition))

      partitionsWithOfflineFutureReplica.foreach(partition => partition.removeFutureLocalReplica(deleteFromLogDir = false))
      newOfflinePartitions.foreach { topicPartition =>
        val partition = allPartitions.put(topicPartition, ReplicaManager.OfflinePartition)
        partition.removePartitionMetrics()
      }
      newOfflinePartitions.map(_.topic).foreach { topic: String =>
        val topicHasPartitions = allPartitions.values.exists(partition => topic == partition.topic)
        if (!topicHasPartitions)
          brokerTopicStats.removeMetrics(topic)
      }
      highWatermarkCheckpoints = highWatermarkCheckpoints.filterKeys(_ != dir)

      info(s"Broker $localBrokerId stopped fetcher for partitions ${newOfflinePartitions.mkString(",")} and stopped moving logs " +
           s"for partitions ${partitionsWithOfflineFutureReplica.mkString(",")} because they are in the failed log directory $dir.")
    }
    logManager.handleLogDirFailure(dir)

    if (sendZkNotification)
      zkClient.propagateLogDirEvent(localBrokerId)
    info(s"Stopped serving replicas in dir $dir")
  }

  def removeMetrics() {
    removeMetric("LeaderCount")
    removeMetric("PartitionCount")
    removeMetric("OfflineReplicaCount")
    removeMetric("UnderReplicatedPartitions")
    removeMetric("UnderMinIsrPartitionCount")
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true) {
    info("Shutting down")
    removeMetrics()
    if (logDirFailureHandler != null)
      logDirFailureHandler.shutdown()
    replicaFetcherManager.shutdown()
    replicaAlterLogDirsManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    info("Shut down completely")
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String], quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)
  }

  protected def createReplicaAlterLogDirsManager(quotaManager: ReplicationQuotaManager, brokerTopicStats: BrokerTopicStats) = {
    new ReplicaAlterLogDirsManager(config, this, quotaManager, brokerTopicStats)
  }

  def lastOffsetForLeaderEpoch(requestedEpochInfo: Map[TopicPartition, Integer]): Map[TopicPartition, EpochEndOffset] = {
    requestedEpochInfo.map { case (tp, leaderEpoch) =>
      val epochEndOffset = getPartition(tp) match {
        case Some(partition) =>
          if (partition eq ReplicaManager.OfflinePartition)
            new EpochEndOffset(KAFKA_STORAGE_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
          else
            partition.lastOffsetForLeaderEpoch(leaderEpoch)
        case None =>
          new EpochEndOffset(UNKNOWN_TOPIC_OR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
      tp -> epochEndOffset
    }
  }
}

