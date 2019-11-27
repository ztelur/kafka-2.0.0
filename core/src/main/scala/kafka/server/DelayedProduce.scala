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
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool

import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long,  // // 对应 topic 分区最后一条消息的 offset
                                  responseStatus: PartitionResponse) { // 记录 ProducerResponse 中的错误码
  /**
    * 标识是否正在等待 ISR 集合中的 follower 副本从 leader 副本同步 requiredOffset 之前的消息
    */
  @volatile var acksPending = false

  override def toString = s"[acksPending: $acksPending, error: ${responseStatus.error.code}, " +
    s"startOffset: ${responseStatus.baseOffset}, requiredOffset: $requiredOffset]"
}

/**
 * The produce metadata maintained by the delayed produce operation
 */
case class ProduceMetadata(produceRequiredAcks: Short, // // 对应 acks 值设置
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = s"[requiredAcks: $produceRequiredAcks, partitionStatus: $produceStatus]"
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
  *
  * 当生产者追加消息到集群时（对应 ProduceRequest 请求），实际上是与对应 topic 分区的
  * leader 副本进行交互，当消息写入 leader 副本成功后，为了保证 leader 节点宕机时消息数据不丢失，
  * 一般需要将消息同步到位于 ISR 集合中的全部 follower 副本，
  * 只有当 ISR 集合中所有的 follower 副本成功完成对当前消息的记录之后才认为本次消息追加操作是成功的。
  * 这里就存在一个延时任务的适用场景，即当消息被成功追加到 leader 副本之后，
  * 我们需要创建一个延时任务等待 ISR 集合中所有的 follower 副本完成同步，
  * 并在同步操作完成之后对生产者的请求进行响应，Kafka 定义了 DelayedProduce 类来处理这类需求。
  *
 */
class DelayedProduce(delayMs: Long, // 延迟时长
                     produceMetadata: ProduceMetadata, // 用于判断 DelayedProduce 是否满足执行条件
                     replicaManager: ReplicaManager, // // 副本管理器
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit, // 回调函数，在任务满足条件或到期时执行
                     lockOpt: Option[Lock] = None)
  extends DelayedOperation(delayMs, lockOpt) {

  // first update the acks pending variable according to the error code】
  /**
    * 依据消息写入 leader 分区操作的错误码对 produceMetadata 的 produceStatus 进行初始化
    */
  produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
    if (status.responseStatus.error == Errors.NONE) {
      // Timeout error state will be cleared when required acks are received
      /**
        * 对应 topic 分区消息写入 leader 副本成功，等待其它副本同步
        */
      status.acksPending = true
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT // 默认错误码
    } else {
      // 对应 topic 分区消息写入 leader 副本失败，无需等待
      status.acksPending = false
    }

    trace(s"Initial partition status for $topicPartition is $status")
  }

  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: This broker is no longer the leader: set an error in response
   * Case B: This broker is the leader:
   *   B.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response
   *   B.2 - Otherwise, set the response with no error.
    *
    * 下面来看一下 DelayedProduce#tryComplete 方法实现，该方法会检测当前延时任务所关注的 topic 分区的运行状态，当满足以下 3 个条件之一时则认为无需再继续等待对应的 topic 分区：
    *
    * 对应 topic 分区的 leader 副本不再位于当前 broker 节点上。
    * 检查 ISR 集合中的所有 follower 副本是否完成同步时出现异常。
    * ISR 集合中所有的 follower 副本完成了同步操作。
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    /**
      * 遍历处理所有的 topic 分区
      */
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // skip those partitions that have already been satisfied
      /**
        * 仅处理正在等待 follower 副本复制的分区
        */
      if (status.acksPending) {
        val (hasEnough, error) = replicaManager.getPartition(topicPartition) match {
          case Some(partition) =>

            /**
              * 检测对应分区本次追加的最后一条消息是否已经被 ISR 集合中所有的 follower 副本同步
              */
            if (partition eq ReplicaManager.OfflinePartition)
              (false, Errors.KAFKA_STORAGE_ERROR)
            else
              partition.checkEnoughReplicasReachOffset(status.requiredOffset)
          case None =>
            // Case A
            /**
              * 找不到对应的分区对象，说明对应分区的 leader 副本已经不在当前 broker 节点上
              */
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
        // Case B.1 || B.2
        /**
          * 出现异常，或所有的 ISR 副本已经同步完成
          */
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }

    // check if every partition has satisfied at least one of case A or B
    /**
      * 如果所有的 topic 分区都已经满足了 DelayedProduce 的执行条件，即不存在等待 ack 的分区，则结束本次延时任务
      */
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        debug(s"Expiring produce request for partition $topicPartition with status $status")
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  override def onComplete() {
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition) {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

