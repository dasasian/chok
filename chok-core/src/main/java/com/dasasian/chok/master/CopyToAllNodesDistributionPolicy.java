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
package com.dasasian.chok.master;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

/**
 * Simple deploy policy which distributes the shards in round robin style.<br>
 * Following features are supported:<br>
 * - initial shard distribution<br>
 * - shard distribution when under replicated<br>
 * - shard removal when over-replicated <br>
 * <br>
 * Missing feature:<br>
 * - shard/node rebalancing<br>
 * <br>
 * TODO jz: node load rebalancing
 */
public class CopyToAllNodesDistributionPolicy implements IDeployPolicy {

    private final static Logger LOG = LoggerFactory.getLogger(CopyToAllNodesDistributionPolicy.class);

    @Override
    public ImmutableSetMultimap<String, String> createDistributionPlan(Set<String> shards,
                                                                       Collection<String> aliveNodes,
                                                                       final ImmutableSetMultimap<String, String> currentShard2NodesMap,
                                                                       final int replicationLevel) {
        if (aliveNodes.size() == 0) {
            throw new IllegalArgumentException("no alive nodes to distribute to");
        }

        final SetMultimap<String, String> newNode2ShardsMap = HashMultimap.create(currentShard2NodesMap.inverse());

        aliveNodes.stream().forEach(node -> newNode2ShardsMap.putAll(node, shards));

        return ImmutableSetMultimap.copyOf(newNode2ShardsMap);
    }

}
