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
package com.dasasian.chok.client;

import com.dasasian.chok.testutil.AbstractTest;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class DefaultNodeSelectionPolicyTest extends AbstractTest {

    @Test
    public void testIndexSpawnsMultipleNodes() throws Exception {
        final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
        final Map<String, List<String>> indexToShards = new HashMap<>();
        addIndex(indexToShards, "indexB", "shardB1", "shardB2");

        addNodes(policy, "shardB1", "node1");
        addNodes(policy, "shardB2", "node2");

        // now check results
        Map<String, List<String>> nodeShardsMap = policy.createNode2ShardsMap(indexToShards.get("indexB"));
        assertEquals(2, nodeShardsMap.size());

        assertEquals(2, extractFoundShards(nodeShardsMap).size());
        assertTrue(nodeShardsMap.get("node1").contains("shardB1"));
        assertTrue(nodeShardsMap.get("node2").contains("shardB2"));
    }

    @Test
    public void testQueryMultipleIndexes() throws Exception {
        final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
        final Map<String, List<String>> indexToShards = new HashMap<>();
        addIndex(indexToShards, "indexA", "shardA1");
        addIndex(indexToShards, "indexB", "shardB1", "shardB2");

        addNodes(policy, "shardA1", "node1", "node2");
        addNodes(policy, "shardB1", "node1");
        addNodes(policy, "shardB2", "node2");

        // now check results
        List<String> shards = new ArrayList<>();
        shards.addAll(indexToShards.get("indexA"));
        shards.addAll(indexToShards.get("indexB"));
        Map<String, List<String>> nodeShardsMap = policy.createNode2ShardsMap(shards);
        assertEquals(2, nodeShardsMap.size());
        assertEquals(3, extractFoundShards(nodeShardsMap).size());
    }

    @Test
    public void testSelection() throws Exception {
        final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
        final Map<String, List<String>> indexToShards = new HashMap<>();
        addIndex(indexToShards, "indexA", "shardA", "shardB");

        addNodes(policy, "shardA", "node1", "node2");
        addNodes(policy, "shardB", "node1", "node2");

        Map<String, List<String>> nodeShardsMap1 = policy.createNode2ShardsMap(indexToShards.get("indexA"));
        Map<String, List<String>> nodeShardsMap2 = policy.createNode2ShardsMap(indexToShards.get("indexA"));
        assertEquals(1, nodeShardsMap1.size());
        assertEquals(1, nodeShardsMap2.size());
        assertFalse("nodes should differ", nodeShardsMap1.keySet().equals(nodeShardsMap2.keySet()));
    }

    @Test
    public void testSetShardsAndNodes() throws Exception {
        final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
        final Map<String, List<String>> indexToShards = new HashMap<>();
        addIndex(indexToShards, "indexA", "shardA", "shardB");

        addNodes(policy, "shardA", "node1", "node2");
        addNodes(policy, "shardB", "node1", "node2");

        Map<String, List<String>> nodeShardsMap = policy.createNode2ShardsMap(indexToShards.get("indexA"));
        assertEquals(1, nodeShardsMap.size());

        List<String> shardList = nodeShardsMap.values().iterator().next();
        assertEquals(2, shardList.size());
        assertTrue(shardList.contains("shardA"));
        assertTrue(shardList.contains("shardB"));
    }

    private Set<String> extractFoundShards(Map<String, List<String>> nodeShardsMap) {
        return ImmutableSet.copyOf(Iterables.concat(nodeShardsMap.values()));
    }

    private void addNodes(INodeSelectionPolicy policy, String shardName, String... nodes) {
        policy.update(shardName, Lists.newArrayList(nodes));
    }

    private void addIndex(final Map<String, List<String>> indexToShards, String indexName, String... shards) {
        indexToShards.put(indexName, Lists.newArrayList(shards));
    }

    @Test
    public void testManyShards() throws Exception {
        final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
        final Map<String, List<String>> indexToShards = new HashMap<>();
        addIndex(indexToShards, "indexA", "shardA1", "shardA2", "shardA3");
        addIndex(indexToShards, "indexB", "shardB1", "shardB2", "shardB3");
        addIndex(indexToShards, "indexC", "shardC1", "shardC2", "shardC3", "shardC4");

        addNodes(policy, "shardA1", "node1", "node2", "node3");
        addNodes(policy, "shardA2", "node1", "node2", "node4");
        addNodes(policy, "shardA3", "node1", "node2", "node5");
        addNodes(policy, "shardB1", "node3", "node4", "node5");
        addNodes(policy, "shardB2", "node2", "node4", "node5");
        addNodes(policy, "shardB3", "node1", "node2");
        addNodes(policy, "shardC1", "node2", "node3", "node5");
        addNodes(policy, "shardC2", "node1", "node2", "node4");
        addNodes(policy, "shardC3", "node2", "node4", "node5");
        addNodes(policy, "shardC4", "node1", "node3", "node4");

        List<String> shards = new ArrayList<>();
        shards.addAll(indexToShards.get("indexA"));
        shards.addAll(indexToShards.get("indexB"));
        shards.addAll(indexToShards.get("indexC"));
        Map<String, List<String>> nodeShardsMap = policy.createNode2ShardsMap(shards);
        assertEquals(10, extractFoundShards(nodeShardsMap).size());
    }

}
