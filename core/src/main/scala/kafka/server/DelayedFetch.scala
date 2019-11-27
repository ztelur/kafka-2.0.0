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

import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{NotLeaderForPartitionException, UnknownTopicOrPartitionException, KafkaStorageException}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.IsolationLevel

import scala.collection._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionData) {

  override def toString = "[startOffsetMetadata: " + startOffsetMetadata + ", " +
                          "fetchInfo: " + fetchInfo + "]"
}

/**
 * The fetch metadata maintained by the delayed fetch operation
 */
/**
  * 消费者和 follower 副本均会向目标 topic 分区的 leader
  * 副本所在 broker 节点发送 FetchRequest 请求来拉取消息，
  * 从接收到请求到准备消息数据，再到发送响应之间的过程同样适用于延时任务，
  * Kafka 定义了 DelayedFetch 类来处理这类需求。
  *
  * @param fetchMinBytes
  * @param fetchMaxBytes
  * @param hardMaxBytesLimit
  * @param fetchOnlyLeader
  * @param fetchOnlyCommitted
  * @param isFromFollower
  * @param replicaId
  * @param fetchPartitionStatus
  */
case class FetchMetadata(fetchMinBytes: Int,  // 读取的最小字节数
                         fetchMaxBytes: Int,  // 读取的最大字节数
                         hardMaxBytesLimit: Boolean,
                         fetchOnlyLeader: Boolean, // 是否只读 leader 副本的消息，一般 debug 模式下可以读 follower 副本的数据
                         fetchOnlyCommitted: Boolean, // 是否只读已完成提交的消息（即 HW 之前的消息），如果是来自消费者的请求则该参数是 true，如果是 follower 则该参数是 false
                         isFromFollower: Boolean, // fetch 请求是否来自 follower
                         replicaId: Int,   // fetch 的副本 ID
                         fetchPartitionStatus: Seq[(TopicPartition, FetchPartitionStatus)]) {  // 记录每个 topic 分区的 fetch 状态

  override def toString = "[minBytes: " + fetchMinBytes + ", " +
    "maxBytes:" + fetchMaxBytes + ", " +
    "onlyLeader:" + fetchOnlyLeader + ", " +
    "onlyCommitted: " + fetchOnlyCommitted + ", " +
    "replicaId: " + replicaId + ", " +
    "partitionStatus: " + fetchPartitionStatus + "]"
}
/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory
 */
class DelayedFetch(delayMs: Long,  // 延时任务延迟时长
                   fetchMetadata: FetchMetadata, // 记录对应 topic 分区的状态信息，用于判定当前延时任务是否满足执行条件
                   replicaManager: ReplicaManager, // 副本管理器
                   quota: ReplicaQuota,
                   isolationLevel: IsolationLevel,
                   responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit) // 响应回调函数
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
   *
   * Case A: This broker is no longer the leader for some partitions it tries to fetch
   * Case B: This broker does not know of some partitions it tries to fetch
   * Case C: The fetch offset locates not on the last segment of the log
   * Case D: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
   * Case E: The partition is in an offline log directory on this broker
   *
   * Upon completion, should return whatever data is available for each valid partition
    *
    *
    * 下面来看一下 DelayedFetch#tryComplete 方法的实现，当满足以下 4 个条件之一时会触发执行延时任务：
    *
    * 对应 topic 分区的 leader 副本不再位于当前 broker 节点上。
    * 请求拉取消息的 topic 分区在当前 broker 节点上找不到。
    * 请求拉取消息的 offset 不位于 activeSegment 对象上，可能已经创建了新的 activeSegment，或者 Log 被截断。
    * 累计读取的字节数已经达到所要求的最小字节数。
   */
  override def tryComplete() : Boolean = {
    var accumulatedSize = 0

    /**
      * 遍历处理当前延时任务关注的所有 topic 分区的状态信息
      */
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicPartition, fetchStatus) =>

        /**
          * 获取上次拉取消息的结束 offset
          */
        val fetchOffset = fetchStatus.startOffsetMetadata
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            /**
              * 获取 topic 分区的 leader 副本
              */
            val replica = replicaManager.getLeaderReplicaIfLocal(topicPartition)
            /**
              * 依据发起请求是消费者还是 follower 来确定拉取消息的结束 offset
              */
            val endOffset =
              if (isolationLevel == IsolationLevel.READ_COMMITTED)
                replica.lastStableOffset
              else if (fetchMetadata.fetchOnlyCommitted)
                replica.highWatermark
              else
                replica.logEndOffset

            // Go directly to the check for Case D if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case C.
            /**
              * 校验上次拉取消息完成之后，endOffset 是否发生变化，
              * 如果未发送变化则说明数据不够，没有继续的必要，否则继续执行
              */
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                // Case C, this can happen when the new fetch operation is on a truncated leader
                /**
                  * 条件 3，endOffset 相对于 fetchOffset 较小，说明请求不位于当前 activeSegment 上
                  */
                debug("Satisfying fetch %s since it is fetching later segments of partition %s.".format(fetchMetadata, topicPartition))
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                // Case C, this can happen when the fetch operation is falling behind the current segment
                // or the partition has just rolled a new segment
                /**
                  * 条件 3，fetchOffset 位于 endOffset 之前，但是
                  * fetchOffset 落在老的 LogSegment 上，而非 activeSegment 上
                  */
                debug("Satisfying fetch %s immediately since it is fetching older segments.".format(fetchMetadata))
                // We will not force complete the fetch request if a replica should be throttled.
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                // we take the partition fetch size as upper bound when accumulating the bytes (skip if a throttled partition)
                /**
                  * fetchOffset 和 endOffset 位于同一个 LogSegment 上，计算累计读取的字节数
                  */
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  accumulatedSize += bytesAvailable
              }
            }
          }
        } catch {
          case _: KafkaStorageException => // Case E
            debug("Partition %s is in an offline log directory, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()

          /**
            * 条件 2，请求 fetch 的 topic 分区在当前 broker 节点上找不到
            */
          case _: UnknownTopicOrPartitionException => // Case B
            debug("Broker no longer know of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()

          /**
            * 条件 1，对应 topic 分区的 leader 副本不再位于当前 broker 节点上
            */
          case _: NotLeaderForPartitionException =>  // Case A
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()
        }
    }

    /**
      * 条件 4，累计读取的字节数已经达到最小字节限制
      */
    // Case D
    if (accumulatedSize >= fetchMetadata.fetchMinBytes)
       forceComplete()
    else
      false
  }

  override def onExpiration() {
    if (fetchMetadata.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete() {
    val logReadResults = replicaManager.readFromLocalLog(
      replicaId = fetchMetadata.replicaId,
      fetchOnlyFromLeader = fetchMetadata.fetchOnlyLeader,
      readOnlyCommitted = fetchMetadata.fetchOnlyCommitted,
      fetchMaxBytes = fetchMetadata.fetchMaxBytes,
      hardMaxBytesLimit = fetchMetadata.hardMaxBytesLimit,
      readPartitionInfo = fetchMetadata.fetchPartitionStatus.map { case (tp, status) => tp -> status.fetchInfo },
      quota = quota,
      isolationLevel = isolationLevel)

    val fetchPartitionData = logReadResults.map { case (tp, result) =>
      tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
        result.lastStableOffset, result.info.abortedTransactions)
    }

    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

