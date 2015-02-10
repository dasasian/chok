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

import com.dasasian.chok.testutil.AbstractTest;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LowestShardCountDistributionPolicyTest extends AbstractTest {

    private LowestShardCountDistributionPolicy _distributionPolicy = new LowestShardCountDistributionPolicy();
    Map<String, List<String>> _currentShard2NodesMap = new HashMap<>();
    Map<String, List<String>> _currentNode2ShardsMap = new HashMap<>();

    @Test
    public void testInitialDistribution() throws Exception {
        int replicationLevel = 2;
        List<String> nodes = createNodes("node1", "node2", "node3");
        Set<String> shards = createShards("shard1", "shard2");
        Map<String, List<String>> node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap, _currentNode2ShardsMap, nodes, replicationLevel);
        assertEquals(nodes.size(), node2ShardsMap.size());
        assertSufficientDistribution(replicationLevel, nodes, shards, node2ShardsMap);
    }

    @Test
    public void testInitialDistribution_TooLessNodes() throws Exception {
        List<String> nodes = createNodes("node1");
        Set<String> shards = createShards("shard1", "shard2");
        Map<String, List<String>> node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap, _currentNode2ShardsMap, nodes, 3);
        assertEquals(nodes.size(), node2ShardsMap.size());
        assertEquals(shards.size(), node2ShardsMap.get("node1").size());
    }

    @Test
    public void testUnderReplicatedDistribution() throws Exception {
        int replicationLevel = 3;
        List<String> nodes = createNodes("node1", "node2", "node3");
        Set<String> shards = createShards("shard1", "shard2", "shard3");
        addMapping("shard1", "node1", "node2", "node3");
        addMapping("shard2", "node1");

        Map<String, List<String>> node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap, _currentNode2ShardsMap, nodes, replicationLevel);
        assertEquals(nodes.size(), node2ShardsMap.size());
        assertSufficientDistribution(replicationLevel, nodes, shards, node2ShardsMap);
    }

    @Test
    public void testOverReplicatedDistribution() throws Exception {
        int replicationLevel = 2;
        List<String> nodes = createNodes("node1", "node2", "node3", "node4");
        Set<String> shards = createShards("shard1", "shard2");
        addMapping("shard1", "node1", "node2", "node3", "node4");
        addMapping("shard2", "node1", "node2");

        Map<String, List<String>> node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap, _currentNode2ShardsMap, nodes, replicationLevel);
        assertEquals(nodes.size(), node2ShardsMap.size());
        assertSufficientDistribution(replicationLevel, nodes, shards, node2ShardsMap);
    }

    private void assertSufficientDistribution(int replicationLevel, List<String> nodes, Set<String> shards, Map<String, List<String>> node2ShardsMap) {
        int deployedShardCount = 0;
        for (String node : nodes) {
            deployedShardCount += node2ShardsMap.get(node).size();
            if (deployedShardCount == 0) {
                continue;
            }
            assertTrue(node2ShardsMap.get(node).size() >= 1);
            assertTrue(node2ShardsMap.get(node).size() <= replicationLevel);
        }
        assertEquals(shards.size() * replicationLevel, deployedShardCount);
    }

    private void addMapping(String shard, String... nodes) {
        List<String> shardNodes = _currentShard2NodesMap.get(shard);
        for (String node : nodes) {
            List<String> shards = _currentNode2ShardsMap.get(node);
            shards.add(shard);
            shardNodes.add(node);
        }
    }

    private List<String> createNodes(String... nodeNames) {
        List<String> nodes = new ArrayList<>();
        for (String nodeName : nodeNames) {
            nodes.add(nodeName);
            _currentNode2ShardsMap.put(nodeName, new ArrayList<String>());
        }
        return nodes;
    }

    private Set<String> createShards(String... shardNames) {
        Set<String> shards = new HashSet<>();
        for (String shardName : shardNames) {
            shards.add(shardName);
            _currentShard2NodesMap.put(shardName, new ArrayList<String>());
        }
        return shards;
    }
}
