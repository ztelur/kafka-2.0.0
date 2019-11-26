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
package kafka.coordinator.group

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.OffsetAndMetadata
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch.{NO_PRODUCER_EPOCH, NO_PRODUCER_ID}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, immutable}
import scala.math.max

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 * <p>
 * <b>Delayed operation locking notes:</b>
 * Delayed operations in GroupCoordinator use `group` as the delayed operation
 * lock. ReplicaManager.appendRecords may be invoked while holding the group lock
 * used by its callback.  The delayed callback may acquire the group lock
 * since the delayed operation is completed only if the group lock can be acquired.
  *
  * GroupCoordinator 组件主要功能包括对隶属于同一个 group 的消费者进行分区分配、维护内部 offset topic，以及管理消费者和消费者所属的 group 信息等
  *
 */
class GroupCoordinator(val brokerId: Int, // 所属的 broker 节点的 ID
                       val groupConfig: GroupConfig,  // Group 配置对象，记录了 group 中 session 过期的最小时长和最大时长，即超时时长的合法区间
                       val offsetConfig: OffsetConfig, // 记录 OffsetMetadata 相关的配置项
                       val groupManager: GroupMetadataManager, // 负责管理 group 元数据以及对应的 offset 信息
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat], // 管理 DelayedHeartbeat 延时任务的炼狱
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin], // 管理 DelayedJoin 延时任务的炼狱
                       time: Time) extends Logging {
  import GroupCoordinator._

  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = (Array[Byte], Errors) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableMetadataExpiration: Boolean = true) {
    info("Starting up.")
    groupManager.startup(enableMetadataExpiration)
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  /**
    * 针对来自消费者申请加入指定 group 的 JoinGroupRequest 请求，
    * GroupCoordinator 实例会为 group 中的消费者确定最终的分区分配策略，
    * 并选举新的 group leader 消费者
    * @param groupId
    * @param memberId
    * @param clientId
    * @param clientHost
    * @param rebalanceTimeoutMs
    * @param sessionTimeoutMs
    * @param protocolType
    * @param protocols
    * @param responseCallback
    */
  def handleJoinGroup(groupId: String,
                      memberId: String,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback): Unit = {
    /**
      * 首先会执行一系列的校验操作以保证发送请求的消费者和目标 group 都是合法的
      */
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      responseCallback(joinError(memberId, error))
      return
    }

    if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
      sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT))
    } else {
      // only try to create the group if the group is not unknown AND
      // the member id is UNKNOWN, if member is specified but group does not
      // exist we should reject the request
      /**
        * 获取并处理 group 对应的元数据信息
        */
      groupManager.getGroup(groupId) match {
        /**
          * 对应的 group 不存在
          */
        case None =>
          if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
            // 指定了消费者 ID，但是对应的 group 不存在，则拒绝请求
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          } else {
            // group 不存在，且消费者 ID 未知的情况下，创建 GroupMetadata 对象，并将消费者加入到对应的 group，同时执行分区再均衡操作
            val group = groupManager.addGroup(new GroupMetadata(groupId, initialState = Empty))
            doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          }

        /**
          * 对应的 group 存在，将消费者加入到对应的 group，并执行分区再均衡操作
          */
        case Some(group) =>
          doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
      }
    }
  }

  /**
    * 会校验消费者 ID （如果指定的话）能否被当前 group 识别，以及消费者指定的分区分配策略能否被当前
    * group 支持，如果这些条件都不能满足，则没有必要再继续为该消费者分配分区
    * @param group
    * @param memberId
    * @param clientId
    * @param clientHost
    * @param rebalanceTimeoutMs
    * @param sessionTimeoutMs
    * @param protocolType
    * @param protocols
    * @param responseCallback
    */
  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    group.inLock {
      if (!group.is(Empty) && (!group.protocolType.contains(protocolType) || !group.supportsProtocols(protocols.map(_._1).toSet))) {
        /**
          * 消费者指定的分区分配策略，对应的 group 不支持
          */
        // if the new member does not support the group protocol, reject it
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else if (group.is(Empty) && (protocols.isEmpty || protocolType.isEmpty)) {
        /**
          * 消费者 ID 不能够被识别
          */
        //reject if first member with empty group protocol or protocolType is empty
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // if the member trying to register with a un-recognized id, send the response to let
        // it reset its member id and retry
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
      } else {
        /**
          * 依据 group 的当前状态分别进行处理
          */
        group.currentState match {
          /**
            * 目标 group 已经失效
            */
          case Dead =>
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            /**
              * 对应的 group 的元数据信息已经被删除，说明已经迁移到其它 GroupCoordinator 实例或者不再可用，直接返回错误码
              */
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))

          /**
            * 目标 group 正在执行分区再均衡操作
             */
          case PreparingRebalance =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              /**
                * 对于未知 ID 的消费者申请加入，创建对应的元数据信息，并分配 ID，同时切换 group 的状态为 PreparingRebalance，准备执行分区再分配
                */
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType,
                protocols, group, responseCallback)
            } else {
              /**
                * 对于已知 ID 的消费者重新申请加入，更新对应的元数据信息，同时切换 group 的状态为 PreparingRebalance，准备执行分区再分配
                */
              val member = group.get(memberId)
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }

          /**
            * 目标 group 正在等待 leader 消费者的分区分配结果
            */
          case CompletingRebalance =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              /**
                * 对于未知 ID 的消费者申请加入，创建对应的元数据信息，并分配 ID，同时切换 group 的状态为 PreparingRebalance，准备执行分区再分配
                */
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType,
                protocols, group, responseCallback)
            } else {
              /**
                * 对于已知 ID 的消费者重新申请加入
                */
              val member = group.get(memberId)
              if (member.matches(protocols)) {
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                /**
                  * 分区分配策略未发生变化，返回 GroupMetadata 的信息
                  */
                responseCallback(JoinGroupResult(
                  members = if (group.isLeader(memberId)) {
                    group.currentMemberMetadata
                  } else {
                    Map.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocolOrNull,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              } else {
                /**
                  * 分区分配策略发生变化，更新对应的元数据信息，同时切换 group 的状态为 PreparingRebalance，准备执行分区再分配
                  */
                // member has changed metadata, so force a rebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }
            }

          /**
            * 目标 group 运行正常，或者正在等待 offset 过期
            */
          case Empty | Stable =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              // if the member id is unknown, register the member to the group
              /**
                * 对于未知 ID 的消费者申请加入，创建对应的元数据信息，并分配 ID，同时切换 group 的状态为 PreparingRebalance，准备执行分区再分配
                */
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType,
                protocols, group, responseCallback)
            } else {
              /**
                * 对于已知 ID 的消费者重新申请加入
                */
              val member = group.get(memberId)
              if (group.isLeader(memberId) || !member.matches(protocols)) {
                /**
                  * 当前消费者是 group leader 或支持的分区分配策略发生变化，更新对应的元数据信息，同时切换 group 的状态为 PreparingRebalance，准备执行分区再分配
                  */
                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                // The latter allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              } else {
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                /**
                  * 分区分配策略未发生变化，返回 GroupMetadata 信息
                  */
                responseCallback(JoinGroupResult(
                  members = Map.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocolOrNull,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              }
            }
        }

        /**
          * 如果当前 group 正在准备执行分区再分配，尝试执行 DelayedJoin 延时任务
          */
        if (group.is(PreparingRebalance))
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback): Unit = {
    validateGroupStatus(groupId, ApiKeys.SYNC_GROUP) match {
      case Some(error) if error == Errors.COORDINATOR_LOAD_IN_PROGRESS =>
        // The coordinator is loading, which means we've lost the state of the active rebalance and the
        // group will need to start over at JoinGroup. By returning rebalance in progress, the consumer
        // will attempt to rejoin without needing to rediscover the coordinator. Note that we cannot
        // return COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect the error.
        responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS)

      case Some(error) => responseCallback(Array.empty, error)

      case None =>
        groupManager.getGroup(groupId) match {
          case None => responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
          case Some(group) => doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
        }
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback) {
    group.inLock {
      if (!group.has(memberId)) {
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
      } else if (generationId != group.generationId) {
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION)
      } else {
        group.currentState match {
          case Empty | Dead =>
            responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)

          case PreparingRebalance =>
            responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS)

          case CompletingRebalance =>
            group.get(memberId).awaitingSyncCallback = responseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            if (group.isLeader(memberId)) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the CompletingRebalance state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      resetAndPropagateAssignmentError(group, error)
                      maybePrepareRebalance(group)
                    } else {
                      setAndPropagateAssignment(group, assignment)
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
            }

          case Stable =>
            // if the group is stable, we just return the current assignment
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE)
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }
  }

  def handleLeaveGroup(groupId: String, memberId: String, responseCallback: Errors => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.LEAVE_GROUP).foreach { error =>
      responseCallback(error)
      return
    }

    groupManager.getGroup(groupId) match {
      case None =>
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        responseCallback(Errors.UNKNOWN_MEMBER_ID)

      case Some(group) =>
        group.inLock {
          if (group.is(Dead) || !group.has(memberId)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID)
          } else {
            val member = group.get(memberId)
            removeHeartbeatForLeavingMember(group, member)
            debug(s"Member ${member.memberId} in group ${group.groupId} has left, removing it from the group")
            removeMemberAndUpdateGroup(group, member)
            responseCallback(Errors.NONE)
          }
        }
    }
  }

  def handleDeleteGroups(groupIds: Set[String]): Map[String, Errors] = {
    var groupErrors: Map[String, Errors] = Map()
    var groupsEligibleForDeletion: Seq[GroupMetadata] = Seq()

    groupIds.foreach { groupId =>
      validateGroupStatus(groupId, ApiKeys.DELETE_GROUPS) match {
        case Some(error) =>
          groupErrors += groupId -> error

        case None =>
          groupManager.getGroup(groupId) match {
            case None =>
              groupErrors += groupId ->
                (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
            case Some(group) =>
              group.inLock {
                group.currentState match {
                  case Dead =>
                    groupErrors += groupId ->
                      (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
                  case Empty =>
                    group.transitionTo(Dead)
                    groupsEligibleForDeletion :+= group
                  case _ =>
                    groupErrors += groupId -> Errors.NON_EMPTY_GROUP
                }
              }
          }
      }
    }

    if (groupsEligibleForDeletion.nonEmpty) {
      val offsetsRemoved = groupManager.cleanupGroupMetadata(groupsEligibleForDeletion, _.removeAllOffsets())
      groupErrors ++= groupsEligibleForDeletion.map(_.groupId -> Errors.NONE).toMap
      info(s"The following groups were deleted: ${groupsEligibleForDeletion.map(_.groupId).mkString(", ")}. " +
        s"A total of $offsetsRemoved offsets were removed.")
    }

    groupErrors
  }

  /**
    * 处理消费者发来的心跳请求
    * 该方法首先会校验目标 GroupCoordinator 实例是合法且能够处理当前请求，
    * 然后依据目标 group 的状态对本次心跳请求进行处理。只有当目标 group 处于
    * PreparingRebalance 或 Stable 状态时，且当前消费者确实属于该 group 才能够正常响应请求，
    * 对于处于其它状态的 group 而言只是简单返回对应的错误码
    *
    * @param groupId
    * @param memberId
    * @param generationId
    * @param responseCallback
    */
  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Errors => Unit) {
    validateGroupStatus(groupId, ApiKeys.HEARTBEAT).foreach { error =>
      if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS)
        // the group is still loading, so respond just blindly
        responseCallback(Errors.NONE)
      else
        responseCallback(error)
      return
    }

    groupManager.getGroup(groupId) match {
      case None =>
        responseCallback(Errors.UNKNOWN_MEMBER_ID)

      case Some(group) => group.inLock {
        group.currentState match {
          case Dead =>
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(Errors.UNKNOWN_MEMBER_ID)

          case Empty =>
            responseCallback(Errors.UNKNOWN_MEMBER_ID)

          case CompletingRebalance =>
            if (!group.has(memberId))
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            else
              responseCallback(Errors.REBALANCE_IN_PROGRESS)

          case PreparingRebalance =>
            if (!group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            } else if (generationId != group.generationId) {
              responseCallback(Errors.ILLEGAL_GENERATION)
            } else {
              val member = group.get(memberId)
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.REBALANCE_IN_PROGRESS)
            }

          case Stable =>
            if (!group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            } else if (generationId != group.generationId) {
              responseCallback(Errors.ILLEGAL_GENERATION)
            } else {
              val member = group.get(memberId)
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.NONE)
            }
        }
      }
    }
  }

  def handleTxnCommitOffsets(groupId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                             responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.TXN_OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        val group = groupManager.getGroup(groupId).getOrElse {
          groupManager.addGroup(new GroupMetadata(groupId, initialState = Empty))
        }
        doCommitOffsets(group, NoMemberId, NoGeneration, producerId, producerEpoch, offsetMetadata, responseCallback)
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    validateGroupStatus(groupId, ApiKeys.OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            if (generationId < 0) {
              // the group is not relying on Kafka for group management, so allow the commit
              val group = groupManager.addGroup(new GroupMetadata(groupId, initialState = Empty))
              doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
                offsetMetadata, responseCallback)
            } else {
              // or this is a request coming from an older generation. either way, reject the commit
              responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
            }

          case Some(group) =>
            doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
              offsetMetadata, responseCallback)
        }
    }
  }

  def scheduleHandleTxnCompletion(producerId: Long,
                                  offsetsPartitions: Iterable[TopicPartition],
                                  transactionResult: TransactionResult) {
    require(offsetsPartitions.forall(_.topic == Topic.GROUP_METADATA_TOPIC_NAME))
    val isCommit = transactionResult == TransactionResult.COMMIT
    groupManager.scheduleHandleTxnCompletion(producerId, offsetsPartitions.map(_.partition).toSet, isCommit)
  }

  private def doCommitOffsets(group: GroupMetadata,
                              memberId: String,
                              generationId: Int,
                              producerId: Long,
                              producerEpoch: Short,
                              offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                              responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    group.inLock {
      if (group.is(Dead)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if ((generationId < 0 && group.is(Empty)) || (producerId != NO_PRODUCER_ID)) {
        // The group is only using Kafka to store offsets.
        // Also, for transactional offset commits we don't need to validate group membership and the generation.
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback, producerId, producerEpoch)
      } else if (group.is(CompletingRebalance)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS))
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
      } else {
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)
      }
    }
  }

  def handleFetchOffsets(groupId: String, partitions: Option[Seq[TopicPartition]] = None):
  (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {

    validateGroupStatus(groupId, ApiKeys.OFFSET_FETCH) match {
      case Some(error) => error -> Map.empty
      case None =>
        // return offsets blindly regardless the current group state since the group may be using
        // Kafka commit storage without automatic group management
        (Errors.NONE, groupManager.getOffsets(groupId, partitions))
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading) Errors.COORDINATOR_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    validateGroupStatus(groupId, ApiKeys.DESCRIBE_GROUPS) match {
      case Some(error) => (error, GroupCoordinator.EmptyGroup)
      case None =>
        groupManager.getGroup(groupId) match {
          case None => (Errors.NONE, GroupCoordinator.DeadGroup)
          case Some(group) =>
            group.inLock {
              (Errors.NONE, group.summary)
            }
        }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]) {
    val offsetsRemoved = groupManager.cleanupGroupMetadata(groupManager.currentGroups, group => {
      group.removeOffsets(topicPartitions)
    })
    info(s"Removed $offsetsRemoved offsets associated with deleted partitions: ${topicPartitions.mkString(", ")}.")
  }

  private def isValidGroupId(groupId: String, api: ApiKeys): Boolean = {
    api match {
      case ApiKeys.OFFSET_COMMIT | ApiKeys.OFFSET_FETCH | ApiKeys.DESCRIBE_GROUPS | ApiKeys.DELETE_GROUPS =>
        // For backwards compatibility, we support the offset commit APIs for the empty groupId, and also
        // in DescribeGroups and DeleteGroups so that users can view and delete state of all groups.
        groupId != null
      case _ =>
        // The remaining APIs are groups using Kafka for group coordination and must have a non-empty groupId
        groupId != null && !groupId.isEmpty
    }
  }

  /**
   * Check that the groupId is valid, assigned to this coordinator and that the group has been loaded.
   */
  private def validateGroupStatus(groupId: String, api: ApiKeys): Option[Errors] = {
    if (!isValidGroupId(groupId, api))
      Some(Errors.INVALID_GROUP_ID)
    else if (!isActive.get)
      Some(Errors.COORDINATOR_NOT_AVAILABLE)
    else if (isCoordinatorLoadInProgress(groupId))
      Some(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    else if (!isCoordinatorForGroup(groupId))
      Some(Errors.NOT_COORDINATOR)
    else
      None
  }

  /**
    *
    * @param group
    */
  private def onGroupUnloaded(group: GroupMetadata) {
    group.inLock {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState

      /**
        *  将当前 group 切换成 Dead 状态
        */
      group.transitionTo(Dead)

      /**
        * 依据前置状态分别处理
        */
      previousState match {
        case Empty | Dead =>
        case PreparingRebalance =>

          /**
            * 遍历响应所有消费者的 JoinGroupRequest 请求，返回 NOT_COORDINATOR_FOR_GROUP 错误码
            */
          for (member <- group.allMemberMetadata) {
            if (member.awaitingJoinCallback != null) {
              member.awaitingJoinCallback(joinError(member.memberId, Errors.NOT_COORDINATOR))
              member.awaitingJoinCallback = null
            }
          }

          /**
            * 尝试执行 DelayedJoin 延时任务
            */
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | CompletingRebalance =>

          /**
            * 遍历响应所有消费者的 JoinGroupRequest 请求，返回 NOT_COORDINATOR_FOR_GROUP 错误码
            */
          for (member <- group.allMemberMetadata) {
            if (member.awaitingSyncCallback != null) {
              member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR)
              member.awaitingSyncCallback = null
            }

            /**
              * 尝试执行 DelayHeartbeat 延时任务
              */
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  private def onGroupLoaded(group: GroupMetadata) {
    group.inLock {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable) || group.is(Empty))

      /**
        * 遍历更新当前 group 名下所有消费者的心跳信息
        */
      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int) {
    groupManager.scheduleLoadGroupAndOffsets(offsetTopicPartitionId, onGroupLoaded)
  }

  /**
    * GroupCoordinator 实例开始维护 offset topic 某个分区的 follower 副本的执行逻辑
    * @param offsetTopicPartitionId
    */
  def handleGroupEmigration(offsetTopicPartitionId: Int) {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
    assert(group.is(CompletingRebalance))
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    propagateAssignment(group, Errors.NONE)
  }

  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors) {
    assert(group.is(CompletingRebalance))
    group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])
    propagateAssignment(group, error)
  }

  private def propagateAssignment(group: GroupMetadata, error: Errors) {
    for (member <- group.allMemberMetadata) {
      if (member.awaitingSyncCallback != null) {
        member.awaitingSyncCallback(member.assignment, error)
        member.awaitingSyncCallback = null

        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  private def joinError(memberId: String, error: Errors): JoinGroupResult = {
    JoinGroupResult(
      members = Map.empty,
      memberId = memberId,
      generationId = 0,
      subProtocol = GroupCoordinator.NoProtocol,
      leaderId = GroupCoordinator.NoLeader,
      error = error)
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
    * 正常响应 HeartbeatRequest 请求
    *
    * 对于 HeartbeatRequest 请求的正常响应会更新当前消费者的最近一次心跳时间，
    * 并尝试完成关注该消费者的 DelayedHeartbeat 延时任务，同时创建新的 DelayedHeartbeat 延时任务，
    * 延迟时间为下次心跳超时时间。在整个 GroupCoordinator 实现中有多个地方调用了上述方法，
    * 这也意味着心跳机制不单单依赖于 HeartbeatRequest 请求，实际上只要是消费者发往 GroupCoordinator
    * 的请求都可以携带心跳信息，例如 JoinGroupRequest、SyncGroupRequest，以及 OffsetCommitRequest 等等
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    // complete current heartbeat expectation
    /**
      * 更新对应消费者的心跳时间
      */
    member.latestHeartbeat = time.milliseconds()
    /**
      * 获取 DelayedHeartbeat 延时任务关注的消费者
      */
    val memberKey = MemberKey(member.groupId, member.memberId)

    /**
      * 尝试完成之前添加的 DelayedHeartbeat 延时任务
      */
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    /**
      * 计算下一次的心跳超时时间
      */
    val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs
    /**
      * 创建新的 DelayedHeartbeat 延时任务，并添加到炼狱中进行管理
      */
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  /**
    * 为消费者创建元数据信息并分配 ID，并将对应的消费者元数据信息记录到 group 元数据信息中
    * @param rebalanceTimeoutMs
    * @param sessionTimeoutMs
    * @param clientId
    * @param clientHost
    * @param protocolType
    * @param protocols
    * @param group
    * @param callback
    * @return
    */
  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback) = {
    /**
      * 基于 UUID 生成消费者的 ID
      */
    val memberId = clientId + "-" + group.generateMemberIdSuffix
    /**
      * 创建新的 MemberMetadata 元数据信息对象
      */
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)

    /**
      * 设置回调函数，即 KafkaApis#sendResponseCallback 方法，用于向客户端发送 JoinGroupResponse 响应
      */
    member.awaitingJoinCallback = callback
    // update the newMemberAdded flag to indicate that the join group can be further delayed
    if (group.is(PreparingRebalance) && group.generationId == 0)
      group.newMemberAdded = true

    /**
      * 添加到 GroupMetadata 中，第一个加入 group 的消费者成为 leader 角色
      *
      * 如果当前 group 名下还未选举 leader 消费者，则以第一个加入到当前 group 的消费者作为 leader 角色
      */
    group.add(member)

    /**
      * 尝试切换 group 的状态为 PreparingRebalance
      */
    maybePrepareRebalance(group)
    member
  }

  /**
    * 更新消费者的分区分配策略和响应回调函数
    * @param group
    * @param member
    * @param protocols
    * @param callback
    */
  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    /**
      * 更新 MemberMetadata 支持的协议
      */
    member.supportedProtocols = protocols

    /**
      * 更新 MemberMetadata 的响应回调函数
      */
    member.awaitingJoinCallback = callback

    /**
      * 尝试执行状态切换
      */
    maybePrepareRebalance(group)
  }

  /**
    * 切换 group 的状态为 PreparingRebalance，并创建相应的 DelayedJoin 延时任务，
    * 等待 group 名下所有的消费者发送 JoinGroupRequest 请求申请加入到当前 group 中。
    * @param group
    */
  private def maybePrepareRebalance(group: GroupMetadata) {
    group.inLock {
      if (group.canRebalance)
        prepareRebalance(group)
    }
  }

  /**
    * 然后切换 group 的状态为 PreparingRebalance，表示开始准备执行分区再分配，并创建 DelayedJoin 延时任务等待 group 名下所有消费者发送 JoinGroupRequest 请求申请加入当前 group。
    * @param group
    */
  private def prepareRebalance(group: GroupMetadata) {
    // if any members are awaiting sync, cancel their request and have them rejoin
    /**
      * 如果处于 AwaitingSync 状态，说明在等待 leader 消费者的分区分配结果，
      * 此时对于来自 follower 的 SyncGroupRequest 请求，直接响应 REBALANCE_IN_PROGRESS 错误
      */
    if (group.is(CompletingRebalance))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    val delayedRebalance = if (group.is(Empty))
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    else
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)

    /**
      * 将 group 状态切换成 PreparingRebalance 状态，准备执行分区再分配操作
      */
    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} with old generation ${group.generationId} " +
      s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

    val groupKey = GroupKey(group.groupId)

    /**
      * 将延时任务添加到炼狱中进行管理
      */
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  /**
    * 处理消费者下线
    * @param group
    * @param member
    */
  private def removeMemberAndUpdateGroup(group: GroupMetadata, member: MemberMetadata) {
    /**
      * 将对应的消费者从 GroupMetadata 中删除
      */
    group.remove(member.memberId)
    group.currentState match {
      /**
        * 对应 group 已经失效，什么也不做
        */
      case Dead | Empty =>

      /**
        * 之前的分区分配结果可能已经失效，切换 GroupMetadata 状态为 PreparingRebalance，准备再次重新分配分区
        */
      case Stable | CompletingRebalance => maybePrepareRebalance(group)

      /**
        * 某个消费者下线，可能满足关注该 group 的 DelayedJoin 的执行条件，尝试执行
        */
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group.inLock {
      /**
        * 判断所有已知的消费者是否是否都已经申请加入，
        * 基于 awaitingJoinCallback 回调函数，只有发送了 JoinGroupRequest 请求的消费者才会设置该回调
        */
      if (group.notYetRejoinedMembers.isEmpty)
        forceComplete()
      else false
    }
  }

  def onExpireJoin() {
    // TODO: add metrics for restabilize timeouts
  }

  /**
    * 当延时任务 DelayedJoin 被执行时会触发调用 GroupCoordinator#onCompleteJoin 方法
    * @param group
    */
  def onCompleteJoin(group: GroupMetadata) {
    group.inLock {
      // remove any members who haven't joined the group yet
      /**
        * 移除那些已知的但是未申请重新加入当前 group 的消费者
        */
      group.notYetRejoinedMembers.foreach { failedMember =>
        removeHeartbeatForLeavingMember(group, failedMember)
        group.remove(failedMember.memberId)
        // TODO: cut the socket connection to the client
      }

      if (!group.is(Dead)) {
        /**
          * 递增 group 的年代信息，并选择 group 最终使用的分区分配策略，
          * 如果 group 名下存在消费者则切换状态为 AwaitingSync，否则切换成 Empty
          */
        group.initNextGeneration()
        if (group.is(Empty)) {
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          /**
            * 如果 group 名下已经没有消费者，将空的分区分配信息记录到 offset topic
            */
          groupManager.storeGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId} " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          // trigger the awaiting join group response callback for all the members after rebalancing
          /**
            * 向 group 名下所有的消费者发送 JoinGroupResponse 响应，
            */
          for (member <- group.allMemberMetadata) {
            assert(member.awaitingJoinCallback != null)
            val joinResult = JoinGroupResult(
              members = if (group.isLeader(member.memberId)) {
                group.currentMemberMetadata
              } else {
                Map.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              subProtocol = group.protocolOrNull,
              leaderId = group.leaderOrNull,
              error = Errors.NONE)

            /**
              * 该回调函数在 KafkaApis#handleJoinGroupRequest 中定义（对应 sendResponseCallback 方法），用于将响应对象放入 channel 中等待发送
              */
            member.awaitingJoinCallback(joinResult)
            member.awaitingJoinCallback = null

            /**
              * 心跳机制
              */
            completeAndScheduleNextHeartbeatExpiration(group, member)
          }
        }
      }
    }
  }

  /**
    * 处理心跳的完成
    *
    * @param group
    * @param member
    * @param heartbeatDeadline
    * @param forceComplete
    * @return
    *
    * 如果满足以下 3 个条件之一则强制执行 DelayedHeartbeat 延时任务，表示对应消费者心跳正常：
    *
    * 消费者正在等待 JoinGroupResponse 或 SyncGroupResponse 响应。
    * 消费者最近一次心跳时间距离延时任务到期时间在消费者会话超时时间范围内。
    * 消费者已经离开之前所属的 group。
    */
  def tryCompleteHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group.inLock {
      if (shouldKeepMemberAlive(member, heartbeatDeadline) || member.isLeaving)
        forceComplete()
      else false
    }
  }

  /**
    * 检测当前消费者是否已经离线
    * 如果是则依据所属 group 的当前状态执行：
    *
    * 如果目标 group 已经失效（Dead/Empty），则什么也不做；
    * 如果目标 group 处于正常运行状态（Stable），或者正在等待 leader 消费者的分区分配结果（AwaitingSync），则因当前消费者的下线可能导致之前的分区分配结果已经失效，所以需要重新分配分区；
    * 如果目标 group 处于准备执行分区再分配状态（PreparingRebalance），则无需请求再次重新分配分区，但是因为当前消费者的下线，可能让关注目标 group 的 DelayedJoin 延时任务满足执行条件，所以尝试执行。
    *
    * @param group
    * @param member
    * @param heartbeatDeadline
    */
  def onExpireHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long) {
    group.inLock {
      if (!shouldKeepMemberAlive(member, heartbeatDeadline)) {
        info(s"Member ${member.memberId} in group ${group.groupId} has failed, removing it from the group")
        removeMemberAndUpdateGroup(group, member)
      }
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  private def shouldKeepMemberAlive(member: MemberMetadata, heartbeatDeadline: Long) =
    member.awaitingJoinCallback != null ||
      member.awaitingSyncCallback != null ||
      member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoGeneration = -1
  val NoMemberId = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            time: Time): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkClient, replicaManager, heartbeatPurgatory, joinPurgatory, time)
  }

  private[group] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs,
      groupInitialRebalanceDelayMs = config.groupInitialRebalanceDelay)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkClient, time)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time)
  }

}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int,
                       groupInitialRebalanceDelayMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           error: Errors)
