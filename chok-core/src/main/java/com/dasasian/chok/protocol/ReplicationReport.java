/**
 * Copyright (C) 2014 Dasasian (damith@dasasian.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dasasian.chok.protocol;

import java.util.Map;

public class ReplicationReport {

    private final int desiredReplicationCount;
    private final int minimalShardReplicationCount;
    private final int maximalShardReplicationCount;
    private final Map<String, Integer> shard2ReplicationCount;

    public ReplicationReport(Map<String, Integer> replicationCountByShardMap, int desiredReplicationCount, int minimalShardReplicationCount, int maximalShardReplicationCount) {
        shard2ReplicationCount = replicationCountByShardMap;
        this.desiredReplicationCount = desiredReplicationCount;
        this.minimalShardReplicationCount = minimalShardReplicationCount;
        this.maximalShardReplicationCount = maximalShardReplicationCount;
    }

    public int getReplicationCount(String shardName) {
        return shard2ReplicationCount.get(shardName);
    }

    public int getDesiredReplicationCount() {
        return desiredReplicationCount;
    }

    public int getMinimalShardReplicationCount() {
        return minimalShardReplicationCount;
    }

    public int getMaximalShardReplicationCount() {
        return maximalShardReplicationCount;
    }

    public boolean isUnderreplicated() {
        return getMinimalShardReplicationCount() < getDesiredReplicationCount();
    }

    public boolean isOverreplicated() {
        return getMaximalShardReplicationCount() > getDesiredReplicationCount();
    }

    public boolean isBalanced() {
        return !isUnderreplicated() && !isOverreplicated();
    }

    /**
     * @return true if each shard is deployed at least once
     */
    public boolean isDeployed() {
        return getMinimalShardReplicationCount() > 0;
    }

    @Override
    public String toString() {
        return String.format("desiredReplication: %s | minimalShardReplication: %s | maximalShardReplication: %s", desiredReplicationCount, minimalShardReplicationCount, maximalShardReplicationCount);
    }

}
