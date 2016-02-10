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

import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

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
public class DefaultDistributionPolicy implements IDeployPolicy {

    private final static Logger LOG = LoggerFactory.getLogger(DefaultDistributionPolicy.class);

    @Override
    public ImmutableSetMultimap<String, String> createDistributionPlan(Set<String> shards,
                                                                       Collection<String> aliveNodes,
                                                                       final ImmutableSetMultimap<String, String> currentShard2NodesMap,
                                                                       final int replicationLevel) {
        if (aliveNodes.size() == 0) {
            throw new IllegalArgumentException("no alive nodes to distribute to");
        }

        final SetMultimap<String, String> newNode2ShardsMap = HashMultimap.create(currentShard2NodesMap.inverse());

        shards.stream()
                .sorted((n1, n2) -> Integer.compare(newNode2ShardsMap.get(n1).size(), newNode2ShardsMap.get(n2).size()))
                .forEach(shard -> {
                    int neededDeployments = replicationLevel - currentShard2NodesMap.get(shard).size();
                    if (neededDeployments < 0) {
                        LOG.info("found shard '" + shard + "' over-replicated");
                        // TODO jz: maybe we should add a configurable threshold tha e.g. 10%
                        // over replication is ok ?
                        removeOverreplicatedShards(currentShard2NodesMap, newNode2ShardsMap, shard, neededDeployments);
                    } else if (neededDeployments > 0) {
                        Stream<String> availableNodes = getSortedAvailableNodes(aliveNodes, newNode2ShardsMap, currentShard2NodesMap.get(shard));
                        neededDeployments = chooseNewNodes(newNode2ShardsMap, availableNodes, shard, neededDeployments);
                        if (neededDeployments > 0) {
                            LOG.warn("cannot replicate shard '" + shard + "' " + replicationLevel + " times, cause only " + aliveNodes.size() + " nodes connected");
                        }
                    }
                });

        return ImmutableSetMultimap.copyOf(newNode2ShardsMap);
    }

    private Stream<String> getSortedAvailableNodes(Collection<String> aliveNodes, final Multimap<String, String> node2ShardsMap, Set<String> assignedNodes) {
        return aliveNodes.stream().filter(node -> !assignedNodes.contains(node)).sorted((n1, n2) -> Integer.compare(node2ShardsMap.get(n1).size(), node2ShardsMap.get(n2).size()));
    }

    private int chooseNewNodes(final Multimap<String, String> currentNode2ShardsMap, Stream<String> availableNodes, String shard, int neededDeployments) {
        return neededDeployments - (int) availableNodes.limit(neededDeployments).peek(node -> currentNode2ShardsMap.put(node, shard)).count();
    }

    private void removeOverreplicatedShards(final ImmutableSetMultimap<String, String> currentShard2NodesMap,
                                            final SetMultimap<String, String> newNode2ShardsMap,
                                            String shard, int neededDeployments) {
        while (neededDeployments < 0) {
            String maxShardServingNode = currentShard2NodesMap.get(shard).stream()
                    .max((n1, n2) -> Integer.compare(newNode2ShardsMap.get(n1).size(), newNode2ShardsMap.get(n2).size())).get();
            newNode2ShardsMap.remove(maxShardServingNode, shard);
            neededDeployments++;
        }
    }

}
