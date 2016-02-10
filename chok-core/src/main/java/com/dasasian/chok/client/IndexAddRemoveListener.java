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

import com.dasasian.chok.protocol.ConnectedComponent;
import com.dasasian.chok.protocol.IAddRemoveListener;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.util.ChokException;
import com.dasasian.chok.util.ZkConfiguration;
import com.google.common.base.Predicates;
import com.google.common.collect.*;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Listens for Indexes being added or removed
 *
 * Created by damith.chandrasekara on 8/27/15.
 */
public class IndexAddRemoveListener implements ConnectedComponent, IAddRemoveListener {

    private final static Logger LOG = LoggerFactory.getLogger(IndexAddRemoveListener.class);

    private final Set<String> indicesToWatch = Sets.newConcurrentHashSet();
    private final SetMultimap<String, String> index2Shards = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    private final INodeSelectionPolicy selectionPolicy;
    private final InteractionProtocol protocol;
    private final INodeProxyManager proxyManager;

    public IndexAddRemoveListener(INodeSelectionPolicy selectionPolicy, InteractionProtocol protocol, INodeProxyManager proxyManager) {
        this.selectionPolicy = selectionPolicy;
        this.protocol = protocol;
        this.proxyManager = proxyManager;

        List<String> indexList = this.protocol.registerChildListener(this, ZkConfiguration.PathDef.INDICES_METADATA, this);
        LOG.info("indices=" + indexList);
        addOrWatchNewIndexes(indexList);
    }


    @Override
    public void removed(String name) {
        removeIndex(name);
    }

    @Override
    public void added(String name) {
        addIndex(name);
    }

    private void addIndex(String name) {
        IndexMetaData indexMD = protocol.getIndexMD(name);
        if (isIndexSearchable(indexMD)) {
            addIndexForSearching(indexMD);
        } else {
            addIndexForWatching(name);
        }
    }

    // todo do we need to do this on close?
//    protected void removeIndexes(Iterable<String> indexes) {
//        indexes.forEach(this::removeIndex);
//    }

    protected void removeIndex(String index) {
        if (index2Shards.containsKey(index)) {
            index2Shards.removeAll(index).stream().forEach(shard -> {
                selectionPolicy.remove(shard);
                protocol.unregisterChildListener(this, ZkConfiguration.PathDef.SHARD_TO_NODES, shard);
            });
        } else {
            if (indicesToWatch.contains(index)) {
                protocol.unregisterDataChanges(this, ZkConfiguration.PathDef.INDICES_METADATA, index);
            } else {
                LOG.warn("got remove event for index '" + index + "' but have no shards for it");
            }
        }
    }

    protected void addOrWatchNewIndexes(List<String> indexes) {
        for (String index : indexes) {
            IndexMetaData indexMD = protocol.getIndexMD(index);
            if (indexMD != null) {// could be undeployed in meantime
                if (isIndexSearchable(indexMD)) {
                    addIndexForSearching(indexMD);
                } else {
                    addIndexForWatching(index);
                }
            }
        }
    }

    protected void addIndexForWatching(final String indexName) {
        indicesToWatch.add(indexName);
        protocol.registerDataListener(this, ZkConfiguration.PathDef.INDICES_METADATA, indexName, new IZkDataListener() {
            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                // handled through IndexPathListener
            }

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                IndexMetaData metaData = (IndexMetaData) data;
                if (isIndexSearchable(metaData)) {
                    addIndexForSearching(metaData);
                    protocol.unregisterDataChanges(IndexAddRemoveListener.this, dataPath);
                }
            }
        });
    }

    protected void addIndexForSearching(IndexMetaData indexMD) {
        List<String> shardNames = indexMD.getShards().stream().map(IndexMetaData.Shard::getName).collect(Collectors.toList());

        for (final String shardName : shardNames) {
            List<String> nodes = protocol.registerChildListener(this, ZkConfiguration.PathDef.SHARD_TO_NODES, shardName, new IAddRemoveListener() {
                @Override
                public void removed(String nodeName) {
                    LOG.info("shard '" + shardName + "' removed from node " + nodeName + "'");
                    Iterable<String> shardNodes = Iterables.filter(selectionPolicy.getShardNodes(shardName), Predicates.not(Predicates.equalTo(nodeName)));
                    selectionPolicy.update(shardName, shardNodes);
                }

                @Override
                public void added(String nodeName) {
                    LOG.info("shard '" + shardName + "' added to node '" + nodeName + "'");
                    VersionedProtocol proxy = proxyManager.getProxy(nodeName, true);
                    if (proxy != null) {
                        Iterable<String> shardNodes = Iterables.concat(selectionPolicy.getShardNodes(shardName), ImmutableList.of(nodeName));
                        selectionPolicy.update(shardName, shardNodes);
                    }
                }
            });
            Collection<String> shardNodes = Lists.newArrayList();
            for (String node : nodes) {
                VersionedProtocol proxy = proxyManager.getProxy(node, true);
                if (proxy != null) {
                    shardNodes.add(node);
                }
            }
            selectionPolicy.update(shardName, shardNodes);
            index2Shards.put(indexMD.getName(), shardName);
        }
    }

    protected boolean isIndexSearchable(final IndexMetaData indexMD) {
        if (indexMD.hasDeployError()) {
            return false;
        }
        // TODO jz check replication report ?
        return true;
    }

    @Override
    public void disconnect() {
        // nothing to do - only connection to zk dropped. Proxies might still be
        // available.
    }

    @Override
    public void reconnect() {
        // TODO jz: re-read index information ?
    }

    private Collection<String> getShardsToSearchIn(String[] indexNames) throws ChokException {
        Collection<String> allShards = Sets.newHashSet();
        for (String index : indexNames) {
            if ("*".equals(index)) {
                ImmutableSet.copyOf(index2Shards.values()).forEach(allShards::add);
                break;
            }
            if (index2Shards.containsKey(index)) {
                allShards.addAll(index2Shards.get(index));
            } else {
                Pattern pattern = Pattern.compile(index);
                int matched = 0;
                for (String ind : ImmutableSet.copyOf(index2Shards.keySet())) {
                    if (pattern.matcher(ind).matches()) {
                        allShards.addAll(index2Shards.get(ind));
                        matched++;
                    }
                }
                if (matched == 0) {
                    LOG.warn("No shards found for index name/pattern: " + index);
                }
            }
        }
        if (allShards.isEmpty()) {
            throw new ChokException("Index [pattern(s)] '" + Arrays.toString(indexNames) + "' do not match to any deployed index: " + getIndices());
        }
        return allShards;
    }

    public List<String> getIndices() {
        return ImmutableList.copyOf(index2Shards.keySet());
    }

    public SetMultimap<String, String> getIndex2Shards() {
        return index2Shards;
    }

    public ImmutableSetMultimap<String, String> getNode2ShardsMap(final String[] indexNames) throws ChokException {
        Collection<String> shardsToSearchIn = getShardsToSearchIn(indexNames);
        return selectionPolicy.createNode2ShardsMap(shardsToSearchIn);
    }

    public void close() {
        protocol.unregisterComponent(this);
    }
}
