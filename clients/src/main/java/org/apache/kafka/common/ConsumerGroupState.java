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

package org.apache.kafka.common;

import java.util.HashMap;

/**
 * The consumer group state.
 */
public enum ConsumerGroupState {
    UNKNOWN("Unknown"),
    /**
     * 表示 group 正在准备执行分区再分配操作。
     */
    PREPARING_REBALANCE("PreparingRebalance"),
    /**
     * 表示 group 正在等待 leader 消费者的分区分配结果
     */
    COMPLETING_REBALANCE("CompletingRebalance"),
    /**
     * 表示 group 处于正常运行状态。
     */
    STABLE("Stable"),
    /**
     * 表示 group 名下已经没有消费者，且对应的元数据已经（或正在）被删除。
     */
    DEAD("Dead"),
    /**
     * 表示 group 名下已经没有消费者，并且正在等待记录的所有 offset 元数据过期。
     */
    EMPTY("Empty");

    private final static HashMap<String, ConsumerGroupState> NAME_TO_ENUM;

    static {
        NAME_TO_ENUM = new HashMap<>();
        for (ConsumerGroupState state : ConsumerGroupState.values()) {
            NAME_TO_ENUM.put(state.name, state);
        }
    }

    private final String name;

    ConsumerGroupState(String name) {
        this.name = name;
    }


    /**
     * Parse a string into a consumer group state.
     */
    public static ConsumerGroupState parse(String name) {
        ConsumerGroupState state = NAME_TO_ENUM.get(name);
        return state == null ? UNKNOWN : state;
    }

    @Override
    public String toString() {
        return name;
    }
}
