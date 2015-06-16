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

import com.dasasian.chok.util.CircularList;
import com.dasasian.chok.util.One2ManyListMap;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultNodeSelectionPolicy implements INodeSelectionPolicy {

    private final Map<String, CircularList<String>> shardsToNodeMap = new ConcurrentHashMap<>();

    @Override
    public void update(String shard, Iterable<String> nodes) {
        shardsToNodeMap.put(shard, new CircularList<>(nodes));
    }

    @Override
    public Collection<String> getShardNodes(String shard) {
        CircularList<String> nodeList = shardsToNodeMap.get(shard);
        if (nodeList == null) {
            return Collections.emptyList();
        }
        return nodeList.asList();
    }

    @Override
    public List<String> remove(String shard) {
        CircularList<String> nodes = shardsToNodeMap.remove(shard);
        if (nodes == null) {
            return Collections.emptyList();
        }
        return nodes.asList();
    }

    @Override
    public void removeNode(String node) {
        Set<String> shards = shardsToNodeMap.keySet();
        for (String shard : shards) {
            CircularList<String> nodes = shardsToNodeMap.get(shard);
            synchronized (nodes) {
                nodes.remove(node);
            }
        }
    }

    @Override
    public Map<String, List<String>> createNode2ShardsMap(Iterable<String> shards) throws ShardAccessException {
        One2ManyListMap<String, String> node2ShardsMap = new One2ManyListMap<>();
        for (String shard : shards) {
            CircularList<String> nodeList = shardsToNodeMap.get(shard);
            if (nodeList == null || nodeList.isEmpty()) {
                throw new ShardAccessException(shard);
            }
            String node;
            synchronized (nodeList) {
                node = nodeList.getNext();
            }
            node2ShardsMap.add(node, shard);
        }
        return node2ShardsMap.asMap();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DefaultNodeSelectionPolicy: ");
        String sep = "";
        for (Map.Entry<String, CircularList<String>> e : shardsToNodeMap.entrySet()) {
            builder.append(sep);
            builder.append(e.getKey());
            builder.append(" --> ");
            builder.append(e.getValue());
            sep = " ";
        }
        return builder.toString();
    }

}
