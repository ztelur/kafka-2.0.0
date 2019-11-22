/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class ConsumerCoordinator extends AbstractCoordinator {
    private final Logger log;
    /**
     * 消费者在发送 JoinGroupRequest 请求时会传递自己支持的分区分配策略，服务端会从所有消费者都支持的策略中选择一种
     * 并通知 group leader 使用此分配策略进行分配
     */
    private final List<PartitionAssignor> assignors;
    /**
     * 集群元数据信息
     */
    private final Metadata metadata;
    private final ConsumerCoordinatorMetrics sensors;
    /**
     * 记录 topic 分区和 offset 的对应关系
     */
    private final SubscriptionState subscriptions;
    /**
     * 默认的 offset 提交完成时的 callback
     */
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    /**
     * 是否启用 offset 自动提交策略
     */
    private final boolean autoCommitEnabled;
    /**
     * offset 自动提交时间间隔
     */
    private final int autoCommitIntervalMs;
    /**
     * 注册的拦截器集合
     */
    private final ConsumerInterceptors<?, ?> interceptors;
    /**
     * 是否排除内部 topic，即 offset topic
     */
    private final boolean excludeInternalTopics;
    /**
     * 记录正在等待异步提交 offset 的请求数目
     */
    private final AtomicInteger pendingAsyncCommits;

    // this collection must be thread-safe because it is modified from the response handler
    // of offset commit requests, which may be invoked from the heartbeat thread
    /**
     * 记录每个 offset 提交对应的响应 callback
     */
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;
    /**
     * 标记当前消费者是不是 group leader
     */
    private boolean isLeader = false;
    /**
     * 当前消费者成功订阅的 topic 集合
     */
    private Set<String> joinedSubscription;
    /**
     * 元数据快照，用于检测 topic 分区数量是否发生变化
     */
    private MetadataSnapshot metadataSnapshot;
    /**
     * 元数据快照，用于检测分区分配过程中分区数量是否发生变化
     */
    private MetadataSnapshot assignmentSnapshot;
    /**
     * 下一次自动提交 offset 的截止时间
     */
    private long nextAutoCommitDeadline;

    // hold onto request&future for committed offset requests to enable async calls.
    private PendingCommittedOffsetRequest pendingCommittedOffsetRequest = null;

    private static class PendingCommittedOffsetRequest {
        private final Set<TopicPartition> requestedPartitions;
        private final Generation requestedGeneration;
        private final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response;

        private PendingCommittedOffsetRequest(final Set<TopicPartition> requestedPartitions,
                                              final Generation generationAtRequestTime,
                                              final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response) {
            this.requestedPartitions = Objects.requireNonNull(requestedPartitions);
            this.response = Objects.requireNonNull(response);
            this.requestedGeneration = generationAtRequestTime;
        }

        private boolean sameRequest(final Set<TopicPartition> currentRequest, final Generation currentGeneration) {
            return (requestedGeneration == null ? currentGeneration == null : requestedGeneration.equals(currentGeneration))
                && requestedPartitions.equals(currentRequest);
        }
    }

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(LogContext logContext,
                               ConsumerNetworkClient client,
                               String groupId,
                               int rebalanceTimeoutMs,
                               int sessionTimeoutMs,
                               Heartbeat heartbeat,
                               List<PartitionAssignor> assignors,
                               Metadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs,
                               boolean autoCommitEnabled,
                               int autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean excludeInternalTopics,
                               final boolean leaveGroupOnClose) {
        super(logContext,
              client,
              groupId,
              rebalanceTimeoutMs,
              sessionTimeoutMs,
              heartbeat,
              metrics,
              metricGrpPrefix,
              time,
              retryBackoffMs,
              leaveGroupOnClose);
        this.log = logContext.logger(ConsumerCoordinator.class);
        this.metadata = metadata;
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.assignors = assignors;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.excludeInternalTopics = excludeInternalTopics;
        this.pendingAsyncCommits = new AtomicInteger();

        if (autoCommitEnabled)
            this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;

        this.metadata.requestUpdate();
        addMetadataListener();
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    public List<ProtocolMetadata> metadata() {
        this.joinedSubscription = subscriptions.subscription();
        List<ProtocolMetadata> metadataList = new ArrayList<>();
        for (PartitionAssignor assignor : assignors) {
            Subscription subscription = assignor.subscription(joinedSubscription);
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);
            metadataList.add(new ProtocolMetadata(assignor.name(), metadata));
        }
        return metadataList;
    }

    public void updatePatternSubscription(Cluster cluster) {
        final Set<String> topicsToSubscribe = new HashSet<>();

        for (String topic : cluster.topics())
            if (subscriptions.subscribedPattern().matcher(topic).matches() &&
                    !(excludeInternalTopics && cluster.internalTopics().contains(topic)))
                topicsToSubscribe.add(topic);

        subscriptions.subscribeFromPattern(topicsToSubscribe);

        // note we still need to update the topics contained in the metadata. Although we have
        // specified that all topics should be fetched, only those set explicitly will be retained
        metadata.setTopics(subscriptions.groupSubscription());
    }

    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
                // if we encounter any unauthorized topics, raise an exception to the user
                if (!cluster.unauthorizedTopics().isEmpty())
                    throw new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));

                if (subscriptions.hasPatternSubscription())
                    updatePatternSubscription(cluster);

                // check if there are any changes to the metadata which should trigger a rebalance
                if (subscriptions.partitionsAutoAssigned()) {
                    MetadataSnapshot snapshot = new MetadataSnapshot(subscriptions, cluster);
                    if (!snapshot.equals(metadataSnapshot))
                        metadataSnapshot = snapshot;
                }

                if (!Collections.disjoint(metadata.topics(), unavailableTopics))
                    metadata.requestUpdate();
            }
        });
    }

    private PartitionAssignor lookupAssignor(String name) {
        for (PartitionAssignor assignor : this.assignors) {
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        // only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
        if (!isLeader)
            assignmentSnapshot = null;

        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);
        subscriptions.assignFromSubscribed(assignment.partitions());

        // check if the assignment contains some topics that were not in the original
        // subscription, if yes we will obey what leader has decided and add these topics
        // into the subscriptions as long as they still match the subscribed pattern
        //
        // TODO this part of the logic should be removed once we allow regex on leader assign
        Set<String> addedTopics = new HashSet<>();
        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            if (!joinedSubscription.contains(tp.topic()))
                addedTopics.add(tp.topic());
        }

        if (!addedTopics.isEmpty()) {
            Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
            Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
            newSubscription.addAll(addedTopics);
            newJoinedSubscription.addAll(addedTopics);

            this.subscriptions.subscribeFromPattern(newSubscription);
            this.joinedSubscription = newJoinedSubscription;
        }

        // Update the metadata to include the full group subscription. The leader will trigger a rebalance
        // if there are any metadata changes affecting any of the consumed partitions (whether or not this
        // instance is subscribed to the topics).
        this.metadata.setTopics(subscriptions.groupSubscription());

        // give the assignor a chance to update internal state based on the received assignment
        assignor.onAssignment(assignment);

        // reschedule the auto commit starting from now
        this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;

        // execute the user's callback after rebalance
        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        log.info("Setting newly assigned partitions {}", subscriptions.assignedPartitions());
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsAssigned(assigned);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on partition assignment", listener.getClass().getName(), e);
        }
    }

    /**
     * Poll for coordinator events. This ensures that the coordinator is known and that the consumer
     * has joined the group (if it is using group management). This also handles periodic offset commits
     * if they are enabled.
     * <p>
     * Returns early if the timeout expires
     *
     * @param timeoutMs The amount of time, in ms, allotted for this operation.
     * @return true iff the operation succeeded
     * 从而开始对当前 topic 分区执行再分配
     */
    public boolean poll(final long timeoutMs) {
        final long startTime = time.milliseconds();
        long currentTime = startTime;
        long elapsed = 0L;
        /**
         * 触发执行注册的监听 offset 提交完成的方法
         */
        invokeCompletedOffsetCommitCallbacks();
        /**
         * 确保当前是 AUTO_TOPICS 或 AUTO_PATTERN（USER_ASSIGNED 不需要再平衡）订阅模式，
         * 且目标 GroupCoordinator 节点可达，如果不可达，则会尝试寻找一个可用的节点
         */
        if (subscriptions.partitionsAutoAssigned()) {
            // Always update the heartbeat last poll time so that the heartbeat thread does not leave the
            // group proactively due to application inactivity even if (say) the coordinator cannot be found.
            pollHeartbeat(currentTime);

            if (coordinatorUnknown()) {
                if (!ensureCoordinatorReady(remainingTimeAtLeastZero(timeoutMs, elapsed))) {
                    return false;
                }
                currentTime = time.milliseconds();
                elapsed = currentTime - startTime;
            }
            /**
             * 需要执行再平衡
             */
            if (rejoinNeededOrPending()) {
                // due to a race condition between the initial metadata fetch and the initial rebalance,
                // we need to ensure that the metadata is fresh before joining initially. This ensures
                // that we have matched the pattern against the cluster's topics at least once before joining.
                /**
                 * 如果是 AUTO_PATTERN 订阅模式，则检查是否需要更新集群元数据
                 */
                if (subscriptions.hasPatternSubscription()) {
                    // For consumer group that uses pattern-based subscription, after a topic is created,
                    // any consumer that discovers the topic after metadata refresh can trigger rebalance
                    // across the entire consumer group. Multiple rebalances can be triggered after one topic
                    // creation if consumers refresh metadata at vastly different times. We can significantly
                    // reduce the number of rebalances caused by single topic creation by asking consumer to
                    // refresh metadata before re-joining the group as long as the refresh backoff time has
                    // passed.
                    if (this.metadata.timeToAllowUpdate(currentTime) == 0) {
                        this.metadata.requestUpdate();
                    }
                    if (!client.ensureFreshMetadata(remainingTimeAtLeastZero(timeoutMs, elapsed))) {
                        return false;
                    }
                    currentTime = time.milliseconds();
                    elapsed = currentTime - startTime;
                }
                /**
                 * 检查目标 GroupCoordinator 节点是否准备好接收请求
                 * 启动心跳线程
                 * 执行分区再分配操作
                 */
                if (!ensureActiveGroup(remainingTimeAtLeastZero(timeoutMs, elapsed))) {
                    return false;
                }

                currentTime = time.milliseconds();
            }
        } else {
            // For manually assigned partitions, if there are no ready nodes, await metadata.
            // If connections to all nodes fail, wakeups triggered while attempting to send fetch
            // requests result in polls returning immediately, causing a tight loop of polls. Without
            // the wakeup, poll() with no channels would block for the timeout, delaying re-connection.
            // awaitMetadataUpdate() initiates new connections with configured backoff and avoids the busy loop.
            // When group management is used, metadata wait is already performed for this scenario as
            // coordinator is unknown, hence this check is not required.
            if (metadata.updateRequested() && !client.hasReadyNodes(startTime)) {
                final boolean metadataUpdated = client.awaitMetadataUpdate(remainingTimeAtLeastZero(timeoutMs, elapsed));
                if (!metadataUpdated && !client.hasReadyNodes(time.milliseconds())) {
                    return false;
                }

                currentTime = time.milliseconds();
            }
        }
        /**
         * 异步提交 offset
         */
        maybeAutoCommitOffsetsAsync(currentTime);
        return true;
    }

    private long remainingTimeAtLeastZero(final long timeoutMs, final long elapsed) {
        return Math.max(0, timeoutMs - elapsed);
    }

    /**
     * Return the time to the next needed invocation of {@link #poll(long)}.
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
     */
    public long timeToNextPoll(long now) {
        if (!autoCommitEnabled)
            return timeToNextHeartbeat(now);

        if (now > nextAutoCommitDeadline)
            return 0;

        return Math.min(nextAutoCommitDeadline - now, timeToNextHeartbeat(now));
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        Map<String, ByteBuffer> allSubscriptions) {
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Set<String> allSubscribedTopics = new HashSet<>();
        Map<String, Subscription> subscriptions = new HashMap<>();
        for (Map.Entry<String, ByteBuffer> subscriptionEntry : allSubscriptions.entrySet()) {
            Subscription subscription = ConsumerProtocol.deserializeSubscription(subscriptionEntry.getValue());
            subscriptions.put(subscriptionEntry.getKey(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        this.subscriptions.groupSubscribe(allSubscribedTopics);
        metadata.setTopics(this.subscriptions.groupSubscription());

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        if (!client.ensureFreshMetadata(Long.MAX_VALUE)) throw new TimeoutException();

        isLeader = true;

        log.debug("Performing assignment using strategy {} with subscriptions {}", assignor.name(), subscriptions);

        Map<String, Assignment> assignment = assignor.assign(metadata.fetch(), subscriptions);

        // user-customized assignor may have created some topics that are not in the subscription list
        // and assign their partitions to the members; in this case we would like to update the leader's
        // own metadata with the newly added topics so that it will not trigger a subsequent rebalance
        // when these topics gets updated from metadata refresh.
        //
        // TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol
        //       we may need to modify the PartitionAssignor API to better support this case.
        Set<String> assignedTopics = new HashSet<>();
        for (Assignment assigned : assignment.values()) {
            for (TopicPartition tp : assigned.partitions())
                assignedTopics.add(tp.topic());
        }

        if (!assignedTopics.containsAll(allSubscribedTopics)) {
            Set<String> notAssignedTopics = new HashSet<>(allSubscribedTopics);
            notAssignedTopics.removeAll(assignedTopics);
            log.warn("The following subscribed topics are not assigned to any members: {} ", notAssignedTopics);
        }

        if (!allSubscribedTopics.containsAll(assignedTopics)) {
            Set<String> newlyAddedTopics = new HashSet<>(assignedTopics);
            newlyAddedTopics.removeAll(allSubscribedTopics);
            log.info("The following not-subscribed topics are assigned, and their metadata will be " +
                    "fetched from the brokers: {}", newlyAddedTopics);

            allSubscribedTopics.addAll(assignedTopics);
            this.subscriptions.groupSubscribe(allSubscribedTopics);
            metadata.setTopics(this.subscriptions.groupSubscription());
            if (!client.ensureFreshMetadata(Long.MAX_VALUE)) throw new TimeoutException();
        }

        assignmentSnapshot = metadataSnapshot;

        log.debug("Finished assignment for group: {}", assignment);

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        // commit offsets prior to rebalance if auto-commit enabled
        maybeAutoCommitOffsetsSync(rebalanceTimeoutMs);

        // execute the user's callback before rebalance
        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        log.info("Revoking previously assigned partitions {}", subscriptions.assignedPartitions());
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsRevoked(revoked);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on partition revocation", listener.getClass().getName(), e);
        }

        isLeader = false;
        subscriptions.resetGroupSubscription();
    }

    @Override
    public boolean rejoinNeededOrPending() {
        /**
         * USER_ASSIGNED 订阅模式不需要执行分区再分配
         */
        if (!subscriptions.partitionsAutoAssigned())
            return false;

        // we need to rejoin if we performed the assignment and metadata has changed
        /**
         * 再平衡过程中分区数量发生变化
         */
        if (assignmentSnapshot != null && !assignmentSnapshot.equals(metadataSnapshot))
            return true;

        // we need to join if our subscription has changed since the last join
        /**
         * 消费者 topic 订阅信息发生变化
         */
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription()))
            return true;
        /**
         * 其它标识需要再平衡的操作，例如分区再分配执行失败、重置年代信息等
         */
        return super.rejoinNeededOrPending();
    }

    /**
     * Refresh the committed offsets for provided partitions.
     *
     * @param timeoutMs A time limit for this operation
     * @return true iff the operation completed within the timeout
     */
    public boolean refreshCommittedOffsetsIfNeeded(final long timeoutMs) {
        final Set<TopicPartition> missingFetchPositions = subscriptions.missingFetchPositions();

        final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(missingFetchPositions, timeoutMs);
        if (offsets == null) return false;

        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final long offset = entry.getValue().offset();
            log.debug("Setting offset for partition {} to the committed offset {}", tp, offset);
            this.subscriptions.seek(tp, offset);
        }
        return true;
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     *
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset or null if the operation timed out
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(final Set<TopicPartition> partitions,
                                                                        final long timeoutMs) {
        if (partitions.isEmpty()) return Collections.emptyMap();

        final Generation generation = generation();
        if (pendingCommittedOffsetRequest != null && !pendingCommittedOffsetRequest.sameRequest(partitions, generation)) {
            // if we were waiting for a different request, then just clear it.
            pendingCommittedOffsetRequest = null;
        }

        final long startMs = time.milliseconds();
        long elapsedTime = 0L;

        while (true) {
            if (!ensureCoordinatorReady(remainingTimeAtLeastZero(timeoutMs, elapsedTime))) return null;
            elapsedTime = time.milliseconds() - startMs;

            // contact coordinator to fetch committed offsets
            final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future;
            if (pendingCommittedOffsetRequest != null) {
                future = pendingCommittedOffsetRequest.response;
            } else {
                future = sendOffsetFetchRequest(partitions);
                pendingCommittedOffsetRequest = new PendingCommittedOffsetRequest(partitions, generation, future);

            }
            client.poll(future, remainingTimeAtLeastZero(timeoutMs, elapsedTime));

            if (future.isDone()) {
                pendingCommittedOffsetRequest = null;

                if (future.succeeded()) {
                    return future.value();
                } else if (!future.isRetriable()) {
                    throw future.exception();
                } else {
                    elapsedTime = time.milliseconds() - startMs;
                    final long sleepTime = Math.min(retryBackoffMs, remainingTimeAtLeastZero(startMs, elapsedTime));
                    time.sleep(sleepTime);
                    elapsedTime += sleepTime;
                }
            } else {
                return null;
            }
        }
    }

    public void close(final long timeoutMs) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();

        long now = time.milliseconds();
        final long endTimeMs = now + timeoutMs;
        try {
            maybeAutoCommitOffsetsSync(timeoutMs);
            now = time.milliseconds();
            if (pendingAsyncCommits.get() > 0 && endTimeMs > now) {
                ensureCoordinatorReady(endTimeMs - now);
                now = time.milliseconds();
            }
        } finally {
            super.close(Math.max(0, endTimeMs - now));
        }
    }

    // visible for testing
    void invokeCompletedOffsetCommitCallbacks() {
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null)
                break;
            completion.invoke();
        }
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();

        if (!coordinatorUnknown()) {
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.
            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                    client.pollNoWakeup();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
                            new RetriableCommitFailedException(e)));
                }
            });
        }

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.
        client.pollNoWakeup();
    }

    private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException)
                    commitException = new RetriableCommitFailedException(e);

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
            }
        });
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions. See the exception for more details
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     * @return If the offset commit was successfully sent and a successful response was received from
     *         the coordinator
     *
     *
     * 1 触发注册的监听 offset 提交完成的回调方法；
     * 2 校验待提交的 offset 数据是否为空，如果为空则直接返回；
     * 3 校验目标 GroupCoordinator 实例所在节点是否可用，如果不可用则尝试寻找负载最小且可用的节点；
     * 4 创建并发送提交 offset 的 OffsetCommitRequest 请求；
     * 5 处理请求的响应结果。
     */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, long timeoutMs) {
        /**
         * 触发注册的监听 offset 提交完成的方法
         */
        invokeCompletedOffsetCommitCallbacks();
        /**
         * 如果提交的 offset 数据为空，则直接返回
         */
        if (offsets.isEmpty())
            return true;

        long now = time.milliseconds();
        long startMs = now;
        long remainingMs = timeoutMs;
        do {
            /**
             * 如果目标 GroupCoordinator 节点不可用
             */
            if (coordinatorUnknown()) {
                /**
                 * 尝试寻找负载最小且可用的 GroupCoordinator 节点
                 */
                if (!ensureCoordinatorReady(remainingMs))
                /**
                 * 如果目标 GroupCoordinator 节点未准备好接收请求
                 */
                    return false;

                remainingMs = timeoutMs - (time.milliseconds() - startMs);
            }
            /**
             * 创建并发送 OffsetCommitRequest 请求，提交 offset 值
             */
            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future, remainingMs);

            // We may have had in-flight offset commits when the synchronous commit began. If so, ensure that
            // the corresponding callbacks are invoked prior to returning in order to preserve the order that
            // the offset commits were applied.
            invokeCompletedOffsetCommitCallbacks();
            /**
             * 提交成功
             */
            if (future.succeeded()) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return true;
            }
            /**
             * 提交失败，且不可重试，则抛出异常
             */
            if (future.failed() && !future.isRetriable())
                throw future.exception();

            time.sleep(retryBackoffMs);

            now = time.milliseconds();
            remainingMs = timeoutMs - (now - startMs);
        } while (remainingMs > 0);

        return false;
    }

    public void maybeAutoCommitOffsetsAsync(long now) {
        if (autoCommitEnabled && now >= nextAutoCommitDeadline) {
            this.nextAutoCommitDeadline = now + autoCommitIntervalMs;
            doAutoCommitOffsetsAsync();
        }
    }

    private void doAutoCommitOffsetsAsync() {
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        log.debug("Sending asynchronous auto-commit of offsets {}", allConsumedOffsets);

        commitOffsetsAsync(allConsumedOffsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                    if (exception instanceof RetriableException) {
                        log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}", offsets,
                                exception);
                        nextAutoCommitDeadline = Math.min(time.milliseconds() + retryBackoffMs, nextAutoCommitDeadline);
                    } else {
                        log.warn("Asynchronous auto-commit of offsets {} failed: {}", offsets, exception.getMessage());
                    }
                } else {
                    log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
                }
            }
        });
    }

    /**
     * 该方法的调用时机有两个地方：
     *  执行分区再分配操作的准备阶段（ConsumerCoordinator#onJoinPrepare 方法）；
     *  关闭消费者的时候（ConsumerCoordinator#close 方法，KafkaConsumer#close 方法在执行时会调用该方法）。
     *
     * @param timeoutMs
     */
    private void maybeAutoCommitOffsetsSync(long timeoutMs) {
        if (autoCommitEnabled) {
            /**
             * 获取当前消费者订阅的所有 topic 分区，以及分区对应的消费状态信息
             */
            Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
            try {
                log.debug("Sending synchronous auto-commit of offsets {}", allConsumedOffsets);
                /**
                 * 执行同步 offset 提交
                 */
                if (!commitOffsetsSync(allConsumedOffsets, timeoutMs))
                    log.debug("Auto-commit of offsets {} timed out before completion", allConsumedOffsets);
            } catch (WakeupException | InterruptException e) {
                log.debug("Auto-commit of offsets {} was interrupted before completion", allConsumedOffsets);
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Synchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, e.getMessage());
            }
        }
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     *
     * 1 校验待发送的请求数据是否为空，如果为空则直接返回成功；
     * 2 获取目标 GroupCoordinator 实例所在 broker 节点，并校验其可用性；
     * 3 封装每个目标分区的待提交 offset 数据；
     * 4 获取并校验当前 group 的年代信息，防止提交一些已经离开 group 的消费者的 offset 数据；
     * 5 创建并缓存 OffsetCommitRequest 请求，同时注册响应结果处理器。
     *
     */
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
        /**
         * 如果没有请求的数据，则直接返回
         */
            return RequestFuture.voidSuccess();
        /**
         * 获取 GroupCoordinator 节点，并检查其可达性
         */
        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // create the offset commit request
        /**
         * 封装每个分区对应提交的 offset 数据
         */
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>(offsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata.offset() < 0) {
                /**
                 * 非法的 offset 值
                 */
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
            }
            /**
             * key 是分区，value 是分区对应的请求数据
             */
            offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(
                    offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }
        /**
         * 获取当前消费者所属 group 的年代信息
         */
        final Generation generation;
        if (subscriptions.partitionsAutoAssigned())
        /**
         * 如果是 AUTO_TOPICS 或 AUTO_PATTERN 订阅模式，则获取年代信息
         */
            generation = generation();
        else
        /**
         * 对于 USER_ASSIGNED 模式，因为不涉及到分区再分配操作，所以没有年代信息
         */
            generation = Generation.NO_GENERATION;

        // if the generation is null, we are not part of an active group (and we expect to be).
        // the only thing we can do is fail the commit and let the user rejoin the group in poll()
        if (generation == null)
        /**
         * 如果获取 group 年代信息失败，则说明当前消费者并不属于该 group，抛出异常，需要执行分区再分配
         */
            return RequestFuture.failure(new CommitFailedException());
        /**
         * 创建 OffsetCommitRequest 请求
         */
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(this.groupId, offsetData).
                setGenerationId(generation.generationId).
                setMemberId(generation.memberId).
                setRetentionTime(OffsetCommitRequest.DEFAULT_RETENTION_TIME);

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);
        /**
         * 发送 OffsetCommitRequest 请求，并注册响应处理器
         */
        return client.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets));
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            for (Map.Entry<TopicPartition, Errors> entry : commitResponse.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);
                long offset = offsetAndMetadata.offset();

                Errors error = entry.getValue();
                if (error == Errors.NONE) {
                    log.debug("Committed offset {} for partition {}", offset, tp);
                } else {
                    if (error.exception() instanceof RetriableException) {
                        log.warn("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                    } else {
                        log.error("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                    }

                    if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                        future.raise(new GroupAuthorizationException(groupId));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                            || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                        // raise the error to the user
                        future.raise(error);
                        return;
                    } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS
                            || error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        // just retry
                        future.raise(error);
                        return;
                    } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                            || error == Errors.NOT_COORDINATOR
                            || error == Errors.REQUEST_TIMED_OUT) {
                        markCoordinatorUnknown();
                        future.raise(error);
                        return;
                    } else if (error == Errors.UNKNOWN_MEMBER_ID
                            || error == Errors.ILLEGAL_GENERATION
                            || error == Errors.REBALANCE_IN_PROGRESS) {
                        // need to re-join group
                        resetGeneration();
                        future.raise(new CommitFailedException());
                        return;
                    } else {
                        future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                        return;
                    }
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {}", unauthorizedTopics);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Fetching committed offsets for partitions: {}", partitions);
        // construct the request
        OffsetFetchRequest.Builder requestBuilder = new OffsetFetchRequest.Builder(this.groupId,
                new ArrayList<>(partitions));

        // send the request with a callback
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            if (response.hasError()) {
                Errors error = response.error();
                log.debug("Offset fetch failed: {}", error.message());

                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(error);
                } else if (error == Errors.NOT_COORDINATOR) {
                    // re-discover the coordinator and retry
                    markCoordinatorUnknown();
                    future.raise(error);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                }
                return;
            }

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    Errors error = data.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                    }
                    return;
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
                } else {
                    log.debug("Found no committed offset for partition {}", tp);
                }
            }

            future.complete(offsets);
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final String metricGrpName;
        private final Sensor commitLatency;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitLatency.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));

            Measurable numParts =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        // Get the number of assigned partitions in a thread safe manner
                        return subscriptions.numAssignedPartitions();
                    }
                };
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {
        private final Map<String, Integer> partitionsPerTopic;

        private MetadataSnapshot(SubscriptionState subscription, Cluster cluster) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription())
                partitionsPerTopic.put(topic, cluster.partitionCountForTopic(topic));
            this.partitionsPerTopic = partitionsPerTopic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetadataSnapshot that = (MetadataSnapshot) o;
            return partitionsPerTopic != null ? partitionsPerTopic.equals(that.partitionsPerTopic) : that.partitionsPerTopic == null;
        }

        @Override
        public int hashCode() {
            return partitionsPerTopic != null ? partitionsPerTopic.hashCode() : 0;
        }
    }

    private static class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Exception exception;

        private OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            this.callback = callback;
            this.offsets = offsets;
            this.exception = exception;
        }

        public void invoke() {
            if (callback != null)
                callback.onComplete(offsets, exception);
        }
    }

}
