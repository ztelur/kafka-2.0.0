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
package kafka.cluster


import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge
import kafka.api.{LeaderAndIsr, Request}
import kafka.common.UnexpectedAppendOffsetException
import kafka.controller.KafkaController
import kafka.log.{LogAppendInfo, LogConfig}
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zk.AdminZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ReplicaNotAvailableException, NotEnoughReplicasException, NotLeaderForPartitionException, PolicyViolationException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.{EpochEndOffset, LeaderAndIsrRequest}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.Map

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topic: String, // 分区所属的 topic
                val partitionId: Int, // 分区编号
                time: Time, // 时间戳工具
                replicaManager: ReplicaManager, // 副本管理
                val isOffline: Boolean = false) extends Logging with KafkaMetricsGroup {
  /**
    * topic 分区对象
    */
  val topicPartition = new TopicPartition(topic, partitionId)

  // Do not use replicaManager if this partition is ReplicaManager.OfflinePartition
  /**
    * 当前 broker 的 ID
    */
  private val localBrokerId = if (!isOffline) replicaManager.config.brokerId else -1
  /**
    * 管理分区日志数据
    */
  private val logManager = if (!isOffline) replicaManager.logManager else null
  /**
    * ZK 工具类
    */
  private val zkClient = if (!isOffline) replicaManager.zkClient else null
  // allReplicasMap includes both assigned replicas and the future replica if there is ongoing replica movement
  /**
    * AR 集合，维护当前分区全部副本的集合，key 是副本 ID
    */
  private val allReplicasMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  /**
    *  leader 副本的年代信息
    */
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  // start offset for 'leaderEpoch' above (leader epoch of the current leader for this partition),
  // defined when this broker is leader for partition
  /**
    * leader 副本的 ID
    */
  @volatile private var leaderEpochStartOffsetOpt: Option[Long] = None
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  /**
    * 当前分区的 ISR 集合
    */
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  /**
    * 当前集群控制器的年代信息，会在切换副本角色时进行更新
    */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = s"[Partition $topicPartition broker=$localBrokerId] "

  private def isReplicaLocal(replicaId: Int) : Boolean = replicaId == localBrokerId || replicaId == Request.FutureLocalReplicaId

  private val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  // Do not create metrics if this partition is ReplicaManager.OfflinePartition
  if (!isOffline) {
    newGauge("UnderReplicated",
      new Gauge[Int] {
        def value = {
          if (isUnderReplicated) 1 else 0
        }
      },
      tags
    )

    newGauge("InSyncReplicasCount",
      new Gauge[Int] {
        def value = {
          if (isLeaderReplicaLocal) inSyncReplicas.size else 0
        }
      },
      tags
    )

    newGauge("UnderMinIsr",
      new Gauge[Int] {
        def value = {
          if (isUnderMinIsr) 1 else 0
        }
      },
      tags
    )

    newGauge("ReplicasCount",
      new Gauge[Int] {
        def value = {
          if (isLeaderReplicaLocal) assignedReplicas.size else 0
        }
      },
      tags
    )

    newGauge("LastStableOffsetLag",
      new Gauge[Long] {
        def value = {
          leaderReplicaIfLocal.map { replica =>
            replica.highWatermark.messageOffset - replica.lastStableOffset.messageOffset
          }.getOrElse(0)
        }
      },
      tags
    )
  }

  private def isLeaderReplicaLocal: Boolean = leaderReplicaIfLocal.isDefined

  def isUnderReplicated: Boolean =
    isLeaderReplicaLocal && inSyncReplicas.size < assignedReplicas.size

  def isUnderMinIsr: Boolean = {
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>
        inSyncReplicas.size < leaderReplica.log.get.config.minInSyncReplicas
      case None =>
        false
    }
  }

  /**
    * Create the future replica if 1) the current replica is not in the given log directory and 2) the future replica
    * does not exist. This method assumes that the current replica has already been created.
    *
    * @param logDir log directory
    * @return true iff the future replica is created
    */
  def maybeCreateFutureReplica(logDir: String): Boolean = {
    // The writeLock is needed to make sure that while the caller checks the log directory of the
    // current replica and the existence of the future replica, no other thread can update the log directory of the
    // current replica or remove the future replica.
    inWriteLock(leaderIsrUpdateLock) {
      val currentReplica = getReplica().get
      if (currentReplica.log.get.dir.getParent == logDir)
        false
      else if (getReplica(Request.FutureLocalReplicaId).isDefined) {
        val futureReplicaLogDir = getReplica(Request.FutureLocalReplicaId).get.log.get.dir.getParent
        if (futureReplicaLogDir != logDir)
          throw new IllegalStateException(s"The future log dir $futureReplicaLogDir of $topicPartition is different from the requested log dir $logDir")
        false
      } else {
        getOrCreateReplica(Request.FutureLocalReplicaId)
        true
      }
    }
  }

  /**
    * 当给定的副本 ID 在本地找不到对应的副本 Replica 对象时，会创建一个新的 Replica 对象
    * @param replicaId
    * @param isNew
    * @return
    */
  def getOrCreateReplica(replicaId: Int = localBrokerId, isNew: Boolean = false): Replica = {
    /**
      * 尝试从 AR 集合中获取 replicaId 对应的 Replica 对象，如果不存在则创建一个
      */
    allReplicasMap.getAndMaybePut(replicaId, {
      /**
        * 如果是本地副本
        */
      if (isReplicaLocal(replicaId)) {
        val adminZkClient = new AdminZkClient(zkClient)

        val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
        /**
          * 获取 log 相关配置信息，ZK 中的配置会覆盖默认配置
          */
        val config = LogConfig.fromProps(logManager.currentDefaultConfig.originals, props)
        /**
          * 创建 topic 分区对应的 Log 对象，如果已经存在则直接返回
          */
        val log = logManager.getOrCreateLog(topicPartition, config, isNew, replicaId == Request.FutureLocalReplicaId)
        /**
          * 加载对应 log 目录下的 replication-offset-checkpoint 文件，其中记录了每个 topic 分区的 HW 值
          */
        val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParent)
        val offsetMap = checkpoint.read()
        if (!offsetMap.contains(topicPartition))
          info(s"No checkpointed highwatermark is found for partition $topicPartition")
        /**
          * 获取当前 topic 分区对应的 HW 值，并与 LEO 比较，选择较小的值作为此副本的 HW 位置
          */
        val offset = math.min(offsetMap.getOrElse(topicPartition, 0L), log.logEndOffset)

        /**
          * 创建 Replica 对象
          */
        new Replica(replicaId, topicPartition, time, offset, Some(log))

        /**
          * 如果是远程副本，无需加载本地对应的日志数据
          */
      } else new Replica(replicaId, topicPartition, time)
    })
  }

  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = Option(allReplicasMap.get(replicaId))

  def getReplicaOrException(replicaId: Int = localBrokerId): Replica =
    getReplica(replicaId).getOrElse(
      throw new ReplicaNotAvailableException(s"Replica $replicaId is not available for partition $topicPartition"))

  def leaderReplicaIfLocal: Option[Replica] =
    leaderReplicaIdOpt.filter(_ == localBrokerId).flatMap(getReplica)

  def addReplicaIfNotExists(replica: Replica): Replica =
    allReplicasMap.putIfNotExists(replica.brokerId, replica)

  def assignedReplicas: Set[Replica] =
    allReplicasMap.values.filter(replica => Request.isValidBrokerId(replica.brokerId)).toSet

  def allReplicas: Set[Replica] =
    allReplicasMap.values.toSet

  private def removeReplica(replicaId: Int) {
    allReplicasMap.remove(replicaId)
  }

  def futureReplicaDirChanged(newDestinationDir: String): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      getReplica(Request.FutureLocalReplicaId) match {
        case Some(futureReplica) =>
          if (futureReplica.log.get.dir.getParent != newDestinationDir)
            true
          else
            false
        case None => false
      }
    }
  }

  def removeFutureLocalReplica(deleteFromLogDir: Boolean = true) {
    inWriteLock(leaderIsrUpdateLock) {
      allReplicasMap.remove(Request.FutureLocalReplicaId)
      if (deleteFromLogDir)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  // Return true iff the future replica exists and it has caught up with the current replica for this partition
  // Only ReplicaAlterDirThread will call this method and ReplicaAlterDirThread should remove the partition
  // from its partitionStates if this method returns true
  def maybeReplaceCurrentWithFutureReplica(): Boolean = {
    val replica = getReplica().get
    val futureReplicaLEO = getReplica(Request.FutureLocalReplicaId).map(_.logEndOffset.messageOffset)
    if (futureReplicaLEO.contains(replica.logEndOffset.messageOffset)) {
      // The write lock is needed to make sure that while ReplicaAlterDirThread checks the LEO of the
      // current replica, no other thread can update LEO of the current replica via log truncation or log append operation.
      inWriteLock(leaderIsrUpdateLock) {
        getReplica(Request.FutureLocalReplicaId) match {
          case Some(futureReplica) =>
            if (replica.logEndOffset.messageOffset == futureReplica.logEndOffset.messageOffset) {
              logManager.replaceCurrentWithFutureLog(topicPartition)
              replica.log = futureReplica.log
              futureReplica.log = None
              allReplicasMap.remove(Request.FutureLocalReplicaId)
              true
            } else false
          case None =>
            // Future replica is removed by a non-ReplicaAlterLogDirsThread before this method is called
            // In this case the partition should have been removed from state of the ReplicaAlterLogDirsThread
            // Return false so that ReplicaAlterLogDirsThread does not have to remove this partition from the state again to avoid race condition
            false
        }
      }
    } else false
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      allReplicasMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      leaderEpochStartOffsetOpt = None
      removePartitionMetrics()
      logManager.asyncDelete(topicPartition)
      if (logManager.getLog(topicPartition, isFuture = true).isDefined)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  def getLeaderEpoch: Int = this.leaderEpoch

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  /**
    * makeLeader 用于将本地副本切换成 leader 角色
    * @param controllerId
    * @param partitionStateInfo
    * @param correlationId
    * @return
    */
  def makeLeader(controllerId: Int, partitionStateInfo: LeaderAndIsrRequest.PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      val newAssignedReplicas = partitionStateInfo.basePartitionState.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      /**
        * 更新本地记录的 controller 的年代信息
        */
      controllerEpoch = partitionStateInfo.basePartitionState.controllerEpoch
      // add replicas that are new
      /**
        * 获取/创建请求信息中 AR 和 ISR 集合中所有副本对应的 Replica 对象
        */
      val newInSyncReplicas = partitionStateInfo.basePartitionState.isr.asScala.map(r => getOrCreateReplica(r, partitionStateInfo.isNew)).toSet
      // remove assigned replicas that have been removed by the controller
      /**
        * 移除本地缓存的所有已过期的的副本对象
        */
      (assignedReplicas.map(_.brokerId) -- newAssignedReplicas).foreach(removeReplica)
      inSyncReplicas = newInSyncReplicas
      newAssignedReplicas.foreach(id => getOrCreateReplica(id, partitionStateInfo.isNew))

      val leaderReplica = getReplica().get
      val leaderEpochStartOffset = leaderReplica.logEndOffset.messageOffset
      info(s"$topicPartition starts at Leader Epoch ${partitionStateInfo.basePartitionState.leaderEpoch} from " +
        s"offset $leaderEpochStartOffset. Previous Leader Epoch was: $leaderEpoch")

      //We cache the leader epoch here, persisting it only if it's local (hence having a log dir)
      /**
        * 更新本地记录的分区 leader 副本相关信息
        */
      leaderEpoch = partitionStateInfo.basePartitionState.leaderEpoch // 更新 ISR 集合
      leaderEpochStartOffsetOpt = Some(leaderEpochStartOffset) // 更新 leader 副本的年代信息
      zkVersion = partitionStateInfo.basePartitionState.zkVersion // 更新 ZK 的版本信息

      // In the case of successive leader elections in a short time period, a follower may have
      // entries in its log from a later epoch than any entry in the new leader's log. In order
      // to ensure that these followers can truncate to the right offset, we must cache the new
      // leader epoch and the start offset since it should be larger than any epoch that a follower
      // would try to query.
      leaderReplica.epochs.foreach { epochCache =>
        epochCache.assign(leaderEpoch, leaderEpochStartOffset)
      }
      /**
        * 检测分区 leader 副本是否发生变化
        */
      val isNewLeader = !leaderReplicaIdOpt.contains(localBrokerId)
      val curLeaderLogEndOffset = leaderReplica.logEndOffset.messageOffset
      val curTimeMs = time.milliseconds
      // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
      (assignedReplicas - leaderReplica).foreach { replica =>
        val lastCaughtUpTimeMs = if (inSyncReplicas.contains(replica)) curTimeMs else 0L
        replica.resetLastCaughtUpTime(curLeaderLogEndOffset, curTimeMs, lastCaughtUpTimeMs)
      }

      /**
        * 如果当前 leader 是新选举出来的，则修正 leader 副本的 HW 值，并重置本地缓存的所有远程副本的相关信息
        */
      if (isNewLeader) {
        /**
          *  尝试修正新 leader 副本的 HW 值
          */
        // construct the high watermark metadata for the new leader replica
        leaderReplica.convertHWToLocalOffsetMetadata()
        // mark local replica as the leader after converting hw
        leaderReplicaIdOpt = Some(localBrokerId)
        // reset log end offset for remote replicas
        /**
          * 重置本地缓存的所有远程副本的相关信息
          */
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      // we may need to increment high watermark since ISR could be down to 1
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change and the new epoch is equal or one
   *  greater (that is, no updates have been missed), return false to indicate to the
    * replica manager that state is already correct and the become-follower steps can be skipped
   */
  /**
    *
    * @param controllerId
    * @param partitionStateInfo
    * @param correlationId
    * @return
    */
  def makeFollower(controllerId: Int, partitionStateInfo: LeaderAndIsrRequest.PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val newAssignedReplicas = partitionStateInfo.basePartitionState.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId = partitionStateInfo.basePartitionState.leader
      val oldLeaderEpoch = leaderEpoch
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      /**
        * 更新本地记录的 controller 的年代信息
        */
      controllerEpoch = partitionStateInfo.basePartitionState.controllerEpoch
      // add replicas that are new
      /**
        * 获取/创建请求信息中所有副本对应的 Replica 对象
        */
      newAssignedReplicas.foreach(r => getOrCreateReplica(r, partitionStateInfo.isNew))
      // remove assigned replicas that have been removed by the controller
      /**
        * 移除本地缓存的所有已过期的的副本对象
        */
      (assignedReplicas.map(_.brokerId) -- newAssignedReplicas).foreach(removeReplica)

      /**
        * 更新本地记录的分区 leader 副本相关信息，其中 ISR 集合由 leader 副本维护，将 follower 副本上的 ISR 集合置空
        */
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = partitionStateInfo.basePartitionState.leaderEpoch // 更新 leader 副本的年代信息
      leaderEpochStartOffsetOpt = None
      zkVersion = partitionStateInfo.basePartitionState.zkVersion // 更新 zk 版本信息

      // If the leader is unchanged and the epochs are no more than one change apart, indicate that no follower changes are required
      // Otherwise, we missed a leader epoch update, which means the leader's log may have been truncated prior to the current epoch.
      /**
        * 检测分区 leader 副本是否发生变化，如果发生变化则更新本地记录的 ID 值
        */
      if (leaderReplicaIdOpt.contains(newLeaderBrokerId) && (leaderEpoch == oldLeaderEpoch || leaderEpoch == oldLeaderEpoch + 1)) {
        false
      }
      else {
        /**
          * 发生变化，更新本地记录的分区 leader 副本的 ID
          */
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the follower's state in the leader based on the last fetch request. See
   * [[kafka.cluster.Replica#updateLogReadResult]] for details.
   *
   * @return true if the leader's log start offset or high watermark have been updated
   */
  def updateReplicaLogReadResult(replica: Replica, logReadResult: LogReadResult): Boolean = {
    val replicaId = replica.brokerId
    // No need to calculate low watermark if there is no delayed DeleteRecordsRequest
    val oldLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
    replica.updateLogReadResult(logReadResult)
    val newLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
    // check if the LW of the partition has incremented
    // since the replica's logStartOffset may have incremented
    val leaderLWIncremented = newLeaderLW > oldLeaderLW
    // check if we need to expand ISR to include this replica
    // if it is not in the ISR yet
    val leaderHWIncremented = maybeExpandIsr(replicaId, logReadResult)

    val result = leaderLWIncremented || leaderHWIncremented
    // some delayed operations may be unblocked after HW or LW changed
    if (result)
      tryCompleteDelayedRequests()

    debug(s"Recorded replica $replicaId log end offset (LEO) position ${logReadResult.info.fetchOffsetMetadata.messageOffset}.")
    result
  }

  /**
   * Check and maybe expand the ISR of the partition.
   * A replica will be added to ISR if its LEO >= current hw of the partition and it is caught up to
   * an offset within the current leader epoch. A replica must be caught up to the current leader
   * epoch before it can join ISR, because otherwise, if there is committed data between current
   * leader's HW and LEO, the replica may become the leader before it fetches the committed data
   * and the data will be lost.
   *
   * Technically, a replica shouldn't be in ISR if it hasn't caught up for longer than replicaLagTimeMaxMs,
   * even if its log end offset is >= HW. However, to be consistent with how the follower determines
   * whether a replica is in-sync, we only check HW.
   *
   * This function can be triggered when a replica's LEO has incremented.
   *
   * @return true if the high watermark has been updated
   */
  /**
    * 用于将指定的副本在满足条件下加入到 ISR 集合中，以及依据给定的时间阈值将滞后于 leader 副本超过阈值时间的 follower 副本移出 ISR 集合
    * @param replicaId
    * @param logReadResult
    * @return
    */
  def maybeExpandIsr(replicaId: Int, logReadResult: LogReadResult): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal match {
        /**
          * 只有当本地副本是 leader 副本时，才执行扩张操作，因为 ISR 集合由 leader 副本维护
          */
        case Some(leaderReplica) =>

          /**
            * 获取目标 follower 副本对应的 Replica 对象
            */
          val replica = getReplica(replicaId).get
          /**
            * 获取 leader 副本对应的 HW 值
            */
          val leaderHW = leaderReplica.highWatermark
          val fetchOffset = logReadResult.info.fetchOffsetMetadata.messageOffset

          /**
            * 判断当前 follower 是否应该被加入到 ISR 集合，并在成功加入后更新相关信息
            */
          if (!inSyncReplicas.contains(replica) && // follower 副本不在 ISR 集合中
             assignedReplicas.map(_.brokerId).contains(replicaId) && // AR 集合中包含该 follower 副本
             replica.logEndOffset.offsetDiff(leaderHW) >= 0 &&
             leaderEpochStartOffsetOpt.exists(fetchOffset >= _)) { // follower 副本的 LEO 已经追赶上 leader 副本的 HW 值
            /**
              * 将 follower 副本添加到 ISR 集合中
              */
            val newInSyncReplicas = inSyncReplicas + replica
            info(s"Expanding ISR from ${inSyncReplicas.map(_.brokerId).mkString(",")} " +
              s"to ${newInSyncReplicas.map(_.brokerId).mkString(",")}")
            // update ISR in ZK and cache
            /**
              * 更新 ZK 和本地记录的新的 ISR 集合信息
              */
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }
          // check if the HW of the partition can now be incremented
          // since the replica may already be in the ISR and its LEO has just incremented
          /**
            * 尝试后移 leader 副本的 HW 值
            */
          maybeIncrementLeaderHW(leaderReplica, logReadResult.fetchTimeMs)

        /**
          * 如果不是 leader 副本，啥也不干
          */
        case None => false // nothing to do if no longer leader
      }
    }
  }

  /*
   * Returns a tuple where the first element is a boolean indicating whether enough replicas reached `requiredOffset`
   * and the second element is an error (which would be `Errors.NONE` for no error).
   *
   * Note that this method will only be called if requiredAcks = -1 and we are waiting for all replicas in ISR to be
   * fully caught up to the (local) leader's offset corresponding to this produce request before we acknowledge the
   * produce request.
   */
  /**
    * 检测指定 offset 之前的消息是否已经被 ISR 集合中足够多的 follower 副本确认（ack），以及尝试向后移动 leader 副本的 HW 值
    * @param requiredOffset
    * @return
    */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    leaderReplicaIfLocal match {
      /**
        * 如果当前副本是 leader 副本
        */
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference
        /**
          * 获取 ISR 集合
          */
        val curInSyncReplicas = inSyncReplicas

        if (isTraceEnabled) {
          def logEndOffsetString(r: Replica) = s"broker ${r.brokerId}: ${r.logEndOffset.messageOffset}"
          val (ackedReplicas, awaitingReplicas) = curInSyncReplicas.partition { replica =>
            replica.logEndOffset.messageOffset >= requiredOffset
          }
          trace(s"Progress awaiting ISR acks for offset $requiredOffset: acked: ${ackedReplicas.map(logEndOffsetString)}, " +
            s"awaiting ${awaitingReplicas.map(logEndOffsetString)}")
        }
        /**
          * 对应 min.insync.replicas 配置
          */
        val minIsr = leaderReplica.log.get.config.minInSyncReplicas

        /**
          * 如果当前请求的 offset 小于等于 HW 的 offset
          */
        if (leaderReplica.highWatermark.messageOffset >= requiredOffset) {
          /*
           * The topic may be configured not to accept messages if there are not enough replicas in ISR
           * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
           */
          /**
            * 如果当前分区的 ISR 集合大小大于等于允许的最小值
            */
          if (minIsr <= curInSyncReplicas.size)
            (true, Errors.NONE)
          else

          /**
            * 否则返回 NOT_ENOUGH_REPLICAS_AFTER_APPEND 错误
            */
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else

        /**
          * 如果当前请求的 offset 大于 HW，则直接返回 false，因为 HW 之后的消息对于客户端不可见
          */
          (false, Errors.NONE)
      case None =>

        /**
          * 如果当前副本是 follower 副本，则返回 NOT_LEADER_FOR_PARTITION 错误
          */
        (false, Errors.NOT_LEADER_FOR_PARTITION)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   *
   * The HW is determined by the smallest log end offset among all replicas that are in sync or are considered caught-up.
   * This way, if a replica is considered caught-up, but its log end offset is smaller than HW, we will wait for this
   * replica to catch up to the HW before advancing the HW. This helps the situation when the ISR only includes the
   * leader replica and a follower tries to catch up. If we don't wait for the follower when advancing the HW, the
   * follower's log end offset may keep falling behind the HW (determined by the leader's log end offset) and therefore
   * will never be added to ISR.
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  /**
    * 尝试后移 leader 副本的 HW 位置其核心思想是选取 ISR 集合中副本最小的 LEO 值作为 leader 副本的新 HW 值，
    * 如果计算出来的 HW 值大于 leader 副本当前的 HW 值，则进行更新。考虑到一些位于 ISR 集合之外但是有机会加入 ISR 集合的副本加入 ISR
    * 集合有一个延迟的过程，所以这里也考虑了这些滞后于 leader 副本时间较小的 follower 副本。
    * @param leaderReplica
    * @param curTime
    * @return
    */
  private def maybeIncrementLeaderHW(leaderReplica: Replica, curTime: Long = time.milliseconds): Boolean = {
    val allLogEndOffsets = assignedReplicas.filter { replica =>
      curTime - replica.lastCaughtUpTimeMs <= replicaManager.config.replicaLagTimeMaxMs || inSyncReplicas.contains(replica)
    }.map(_.logEndOffset)
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    val oldHighWatermark = leaderReplica.highWatermark

    // Ensure that the high watermark increases monotonically. We also update the high watermark when the new
    // offset metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset ||
      (oldHighWatermark.messageOffset == newHighWatermark.messageOffset && oldHighWatermark.onOlderSegment(newHighWatermark))) {
      leaderReplica.highWatermark = newHighWatermark
      debug(s"High watermark updated to $newHighWatermark")
      true
    } else {
      def logEndOffsetString(r: Replica) = s"replica ${r.brokerId}: ${r.logEndOffset}"
      debug(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old hw $oldHighWatermark. " +
        s"All current LEOs are ${assignedReplicas.map(logEndOffsetString)}")
      false
    }
  }

  /**
   * The low watermark offset value, calculated only if the local replica is the partition leader
   * It is only used by leader broker to decide when DeleteRecordsRequest is satisfied. Its value is minimum logStartOffset of all live replicas
   * Low watermark will increase when the leader broker receives either FetchRequest or DeleteRecordsRequest.
   */
  def lowWatermarkIfLeader: Long = {
    if (!isLeaderReplicaLocal)
      throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
    val logStartOffsets = allReplicas.collect {
      case replica if replicaManager.metadataCache.isBrokerAlive(replica.brokerId) || replica.brokerId == Request.FutureLocalReplicaId => replica.logStartOffset
    }
    CoreUtils.min(logStartOffsets, 0L)
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(topicPartition)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
    replicaManager.tryCompleteDelayedDeleteRecords(requestKey)
  }

  /**
    *
    * @param replicaMaxLagTimeMs
    */
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {

        /**
          * 只有当本地副本是 leader 副本时，才执行缩减操作，因为 ISR 集合由 leader 副本维护
          */
        case Some(leaderReplica) =>
          /**
            * 从 ISR 集合中获取滞后的 follower 副本集合
            */
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.nonEmpty) {
            /**
              * 将滞后的 follower 副本从 ISR 集合中剔除
              */
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.nonEmpty)
            info("Shrinking ISR from %s to %s".format(inSyncReplicas.map(_.brokerId).mkString(","),
              newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            /**
              * 将新的 ISR 集合信息上报给 ZK，同时更新本地记录的 ISR 集合信息
              */
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1

            replicaManager.isrShrinkRate.mark()

            /**
              * 尝试后移 leader 副本的 HW 值
              */
            maybeIncrementLeaderHW(leaderReplica)
          } else {
            false
          }

        /**
          * 如果不是 leader 副本，则啥也不做
          */
        case None => false // do nothing if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    /**
      * 如果 leader 副本的 HW 值发生变化，尝试执行监听当前 topic 分区的 DelayedFetch 和 DelayedProduce 延时任务
      */
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
     *                     the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *                    then the follower is lagging and should be removed from the ISR
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
     *
     **/
    val candidateReplicas = inSyncReplicas - leaderReplica

    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if (laggingReplicas.nonEmpty)
      debug("Lagging replicas are %s".format(laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }

  private def doAppendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Unit = {
    inReadLock(leaderIsrUpdateLock) {
      if (isFuture) {
        // The read lock is needed to handle race condition if request handler thread tries to
        // remove future replica after receiving AlterReplicaLogDirsRequest.
        inReadLock(leaderIsrUpdateLock) {
          getReplica(Request.FutureLocalReplicaId) match {
            case Some(replica) => replica.log.get.appendAsFollower(records)
            case None => // Future replica is removed by a non-ReplicaAlterLogDirsThread before this method is called
          }
        }
      } else {
        // The read lock is needed to prevent the follower replica from being updated while ReplicaAlterDirThread
        // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
        getReplicaOrException().log.get.appendAsFollower(records)
      }
    }
  }

  def appendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean) {
    try {
      doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
    } catch {
      case e: UnexpectedAppendOffsetException =>
        val replica = if (isFuture) getReplicaOrException(Request.FutureLocalReplicaId) else getReplicaOrException()
        val logEndOffset = replica.logEndOffset.messageOffset
        if (logEndOffset == replica.logStartOffset &&
            e.firstOffset < logEndOffset && e.lastOffset >= logEndOffset) {
          // This may happen if the log start offset on the leader (or current replica) falls in
          // the middle of the batch due to delete records request and the follower tries to
          // fetch its first offset from the leader.
          // We handle this case here instead of Log#append() because we will need to remove the
          // segment that start with log start offset and create a new one with earlier offset
          // (base offset of the batch), which will move recoveryPoint backwards, so we will need
          // to checkpoint the new recovery point before we append
          val replicaName = if (isFuture) "future replica" else "follower"
          info(s"Unexpected offset in append to $topicPartition. First offset ${e.firstOffset} is less than log start offset ${replica.logStartOffset}." +
               s" Since this is the first record to be appended to the $replicaName's log, will start the log from offset ${e.firstOffset}.")
          truncateFullyAndStartAt(e.firstOffset, isFuture)
          doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
        } else
          throw e
    }
  }

  /**
    *
    * @param records
    * @param isFromClient
    * @param requiredAcks
    * @return
    */
  def appendRecordsToLeader(records: MemoryRecords, isFromClient: Boolean, requiredAcks: Int = 0): LogAppendInfo = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        /**
          * 只有 leader 副本支持追加消息操作
          */
        case Some(leaderReplica) =>

          /**
            * 获取 leader 副本对应的 Log 对象
            */
          val log = leaderReplica.log.get
          /**
            * 对应 min.insync.replicas 配置，表示 ISR 集合的最小值
            */
          val minIsr = log.config.minInSyncReplicas
          /**
            * 获取当前分区 ISR 集合的大小
            */
          val inSyncSize = inSyncReplicas.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          /**
            * 如果用户指定 acks = -1，但是当前 ISR 集合小于允许的最小值，则不允许追加消息，防止数据丢失
            */
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException("Number of insync replicas for partition %s is [%d], below required minimum [%d]"
              .format(topicPartition, inSyncSize, minIsr))
          }

          /**
            * 往 leader 副本的 Log 对象中追加消息
            */
          val info = log.appendAsLeader(records, leaderEpoch = this.leaderEpoch, isFromClient)
          // probably unblock some follower fetch requests since log end offset has been updated
          /**
            * 有新的日志数据被追加，尝试执行监听当前 topic 分区的 DelayedFetch 延时任务
            */
          replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1
          /**
            * 尝试后移 leader 副本的 HW 值
            */
          (info, maybeIncrementLeaderHW(leaderReplica))

        /**
          * 如果不是 leader 副本，则抛出异常
          */
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    /**
      * 如果 leader 副本的 HW 值增加了，则尝试执行监听当前 topic 分区的 DelayedFetch 和 DelayedProduce 任务
      */
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    info
  }

  def logStartOffset: Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal.map(_.log.get.logStartOffset).getOrElse(-1)
    }
  }

  /**
   * Update logStartOffset and low watermark if 1) offset <= highWatermark and 2) it is the leader replica.
   * This function can trigger log segment deletion and log rolling.
   *
   * Return low watermark of the partition.
   */
  def deleteRecordsOnLeader(offset: Long): Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          if (!leaderReplica.log.get.config.delete)
            throw new PolicyViolationException("Records of partition %s can not be deleted due to the configured policy".format(topicPartition))
          leaderReplica.maybeIncrementLogStartOffset(offset)
          lowWatermarkIfLeader
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }
  }

  /**
    * Truncate the local log of this partition to the specified offset and checkpoint the recovery point to this offset
    *
    * @param offset offset to be used for truncation
    * @param isFuture True iff the truncation should be performed on the future log of this partition
    */
  def truncateTo(offset: Long, isFuture: Boolean) {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateTo(Map(topicPartition -> offset), isFuture = isFuture)
    }
  }

  /**
    * Delete all data in the local log of this partition and start the log at the new offset
    *
    * @param newOffset The new offset to start the log with
    * @param isFuture True iff the truncation should be performed on the future log of this partition
    */
  def truncateFullyAndStartAt(newOffset: Long, isFuture: Boolean) {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateFullyAndStartAt(topicPartition, newOffset, isFuture = isFuture)
    }
  }

  /**
    * @param leaderEpoch Requested leader epoch
    * @return The requested leader epoch and the end offset of this leader epoch, or if the requested
    *         leader epoch is unknown, the leader epoch less than the requested leader epoch and the end offset
    *         of this leader epoch. The end offset of a leader epoch is defined as the start
    *         offset of the first leader epoch larger than the leader epoch, or else the log end
    *         offset if the leader epoch is the latest leader epoch.
    */
  def lastOffsetForLeaderEpoch(leaderEpoch: Int): EpochEndOffset = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val (epoch, offset) = leaderReplica.endOffsetFor(leaderEpoch)
          new EpochEndOffset(NONE, epoch, offset)
        case None =>
          new EpochEndOffset(NOT_LEADER_FOR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
    }
  }

  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(_.brokerId).toList, zkVersion)
    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topicPartition, newLeaderAndIsr,
      controllerEpoch)

    if (updateSucceeded) {
      replicaManager.recordIsrChange(topicPartition)
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      replicaManager.failedIsrUpdatesRate.mark()
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
    removeMetric("UnderMinIsr", tags)
    removeMetric("InSyncReplicasCount", tags)
    removeMetric("ReplicasCount", tags)
    removeMetric("LastStableOffsetLag", tags)
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic && isOffline == other.isOffline
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId + (if (isOffline) 1 else 0)

  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AllReplicas: " + allReplicasMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString
  }
}
