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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultDistributionPolicyTest {

    private DefaultDistributionPolicy distributionPolicy = new DefaultDistributionPolicy();

    @Test
    public void testInitialDistribution() throws Exception {
        int replicationLevel = 2;
        List<String> nodes = ImmutableList.of("node1", "node2", "node3");
        Set<String> shards = ImmutableSet.of("shard1", "shard2");
        ImmutableSetMultimap<String, String> currentShard2NodesMap = ImmutableSetMultimap.of();
        ImmutableMultimap<String, String> node2ShardsMap = distributionPolicy.createDistributionPlan(shards, nodes, currentShard2NodesMap, replicationLevel);
        assertEquals(nodes.size(), node2ShardsMap.keySet().size());
        assertSufficientDistribution(replicationLevel, nodes, shards, node2ShardsMap);
    }

    @Test
    public void testEqualDistributionOnMultipleSequentialDeploys() throws Exception {
        int replicationLevel = 1;
        List<String> nodes = ImmutableList.of("node1", "node2", "node3", "node4");
        Set<String> shards = ImmutableSet.of("shard1", "shard2");
        ImmutableSetMultimap<String, String> currentShard2NodesMap = ImmutableSetMultimap.of();

        ImmutableMultimap<String, String> node2ShardsMap = distributionPolicy.createDistributionPlan(shards, nodes, currentShard2NodesMap, replicationLevel);
        System.out.println(node2ShardsMap);

        Set<String> shards2 = ImmutableSet.of("shard1", "shard2");
        node2ShardsMap = distributionPolicy.createDistributionPlan(shards2, nodes, currentShard2NodesMap, replicationLevel);
        for (String node : node2ShardsMap.keySet()) {
            assertEquals("shards are not equally distributed: " + node2ShardsMap, 1, node2ShardsMap.get(node).size());
        }
        System.out.println(node2ShardsMap);
    }

    @Test
    public void testInitialDistribution_TooLessNodes() throws Exception {
        int replicationLevel = 3;
        List<String> nodes = ImmutableList.of("node1");
        Set<String> shards = ImmutableSet.of("shard1", "shard2");
        ImmutableSetMultimap<String, String> currentShard2NodesMap = ImmutableSetMultimap.of();
        ImmutableMultimap<String, String> node2ShardsMap = distributionPolicy.createDistributionPlan(shards, nodes, currentShard2NodesMap, replicationLevel);
        assertEquals(nodes.size(), node2ShardsMap.keySet().size());
        assertEquals(shards.size(), node2ShardsMap.get("node1").size());
    }

    @Test
    public void testUnderReplicatedDistribution() throws Exception {
        int replicationLevel = 3;
        List<String> nodes = ImmutableList.of("node1", "node2", "node3");
        Set<String> shards = ImmutableSet.of("shard1", "shard2", "shard3");

        ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();
        builder.putAll("shard1", "node1", "node2", "node3");
        builder.putAll("shard2", "node1");
        ImmutableSetMultimap<String, String> currentShard2NodesMap = builder.build();

        ImmutableMultimap<String, String> node2ShardsMap = distributionPolicy.createDistributionPlan(shards, nodes,
                currentShard2NodesMap, replicationLevel);
        assertEquals(nodes.size(), node2ShardsMap.keySet().size());
        assertSufficientDistribution(replicationLevel, nodes, shards, node2ShardsMap);
    }

    @Test
    public void testOverReplicatedDistribution() throws Exception {
        int replicationLevel = 2;
        List<String> nodes = ImmutableList.of("node1", "node2", "node3", "node4");
        Set<String> shards = ImmutableSet.of("shard1", "shard2");
        ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();
        builder.putAll("shard1", "node1", "node2", "node3", "node4");
        builder.putAll("shard2", "node1", "node2");
        ImmutableSetMultimap<String, String> currentShard2NodesMap = builder.build();

        ImmutableMultimap<String, String> node2ShardsMap = distributionPolicy.createDistributionPlan(shards, nodes,
                currentShard2NodesMap, replicationLevel);
        assertEquals(nodes.size(), node2ShardsMap.keySet().size());
        assertSufficientDistribution(replicationLevel, nodes, shards, node2ShardsMap);
    }

    private void assertSufficientDistribution(int replicationLevel, List<String> nodes, Set<String> shards, ImmutableMultimap<String, String> node2ShardsMap) {
        int deployedShardCount = 0;
        for (String node : nodes) {
            deployedShardCount += node2ShardsMap.get(node).size();
            assertTrue(node2ShardsMap.get(node).size() >= 1);
            assertTrue(node2ShardsMap.get(node).size() <= replicationLevel);
        }
        assertEquals(shards.size() * replicationLevel, deployedShardCount);
    }

}
