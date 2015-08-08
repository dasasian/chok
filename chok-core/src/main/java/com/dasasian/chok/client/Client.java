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
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.dasasian.chok.util.*;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import com.google.common.base.Predicates;
import com.google.common.collect.*;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Client implements ConnectedComponent, AutoCloseable {

    protected final static Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final String[] ALL_INDICES = new String[]{"*"};

    protected final Set<String> indicesToWatch = Sets.newHashSet();
    protected final Multimap<String, String> indexToShards = HashMultimap.create();

    protected final INodeSelectionPolicy selectionPolicy;
    private final long startupTime;
    private final int maxTryCount;
    protected InteractionProtocol protocol;
    private long queryCount = 0;
    private INodeProxyManager proxyManager;

    public Client(Class<? extends VersionedProtocol> serverClass) {
        this(serverClass, new DefaultNodeSelectionPolicy(), ZkConfigurationLoader.loadConfiguration());
    }

    public Client(Class<? extends VersionedProtocol> serverClass, final ZkConfiguration config) {
        this(serverClass, new DefaultNodeSelectionPolicy(), config);
    }

    public Client(Class<? extends VersionedProtocol> serverClass, InteractionProtocol protocol) {
        this(serverClass, new DefaultNodeSelectionPolicy(), protocol, new ClientConfiguration());
    }

    public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy nodeSelectionPolicy) {
        this(serverClass, nodeSelectionPolicy, ZkConfigurationLoader.loadConfiguration());
    }

    public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy policy, final ZkConfiguration zkConfig) {
        this(serverClass, policy, zkConfig, new ClientConfiguration());
    }

    public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy policy, final ZkConfiguration zkConfig, ClientConfiguration clientConfiguration) {
        this(serverClass, policy, new InteractionProtocol(ZkChokUtil.startZkClient(zkConfig, 60000), zkConfig), clientConfiguration);
    }

    public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy policy, final InteractionProtocol protocol, ClientConfiguration clientConfiguration) {
        Set<String> keys = ImmutableSet.copyOf(clientConfiguration.getKeys());
        Configuration hadoopConf = new Configuration();
        synchronized (Configuration.class) {
            for (String key : keys) {
                // simply set all properties / adding non-hadoop properties shouldn't hurt
                hadoopConf.set(key, clientConfiguration.getProperty(key));
            }
        }
        proxyManager = new NodeProxyManager(serverClass, hadoopConf, policy);
        selectionPolicy = policy;
        this.protocol = protocol;
        maxTryCount = clientConfiguration.getInt(ClientConfiguration.CLIENT_NODE_INTERACTION_MAXTRYCOUNT);

        List<String> indexList = this.protocol.registerChildListener(this, PathDef.INDICES_METADATA, new IAddRemoveListener() {
            @Override
            public void removed(String name) {
                removeIndex(name);
            }

            @Override
            public void added(String name) {
                IndexMetaData indexMD = Client.this.protocol.getIndexMD(name);
                if (isIndexSearchable(indexMD)) {
                    addIndexForSearching(indexMD);
                } else {
                    addIndexForWatching(name);
                }
            }
        });
        LOG.info("indices=" + indexList);
        addOrWatchNewIndexes(indexList);
        startupTime = System.currentTimeMillis();
    }

    public INodeSelectionPolicy getSelectionPolicy() {
        return selectionPolicy;
    }

    public INodeProxyManager getProxyManager() {
        return proxyManager;
    }

    public void setProxyCreator(INodeProxyManager proxyManager) {
        this.proxyManager = proxyManager;
    }

    protected void removeIndexes(Iterable<String> indexes) {
        indexes.forEach(this::removeIndex);
    }

    protected void removeIndex(String index) {
        if(indexToShards.containsKey(index)) {
            indexToShards.removeAll(index).stream().forEach(shard -> {
                selectionPolicy.remove(shard);
                protocol.unregisterChildListener(this, PathDef.SHARD_TO_NODES, shard);
            });
        }
        else {
            if (indicesToWatch.contains(index)) {
                protocol.unregisterDataChanges(this, PathDef.INDICES_METADATA, index);
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
        protocol.registerDataListener(this, PathDef.INDICES_METADATA, indexName, new IZkDataListener() {
            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                // handled through IndexPathListener
            }

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                IndexMetaData metaData = (IndexMetaData) data;
                if (isIndexSearchable(metaData)) {
                    addIndexForSearching(metaData);
                    protocol.unregisterDataChanges(Client.this, dataPath);
                }
            }
        });
    }

    protected void addIndexForSearching(IndexMetaData indexMD) {
        List<String> shardNames = indexMD.getShards().stream().map(Shard::getName).collect(Collectors.toList());

        for (final String shardName : shardNames) {
            List<String> nodes = protocol.registerChildListener(this, PathDef.SHARD_TO_NODES, shardName, new IAddRemoveListener() {
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
            indexToShards.put(indexMD.getName(), shardName);
        }
    }

    protected boolean isIndexSearchable(final IndexMetaData indexMD) {
        if (indexMD.hasDeployError()) {
            return false;
        }
        // TODO jz check replication report ?
        return true;
    }

    // --------------- Distributed calls to servers ----------------------

    /**
     * Broadcast a method call to all indices. Return all the results in a
     * Collection.
     *
     * @param <T>                  the class for the result
     * @param timeout              timeout value
     * @param shutdown             ??
     * @param method               The server's method to call.
     * @param shardArrayParamIndex Which parameter of the method call, if any, that should be
     *                             replaced with the shards to search. This is an array of Strings,
     *                             with a different value for each node / server. Pass in -1 to
     *                             disable.
     * @param args                 The arguments to pass to the method when run on the server.
     * @return the results
     * @throws ChokException on exception
     */
    public <T> ClientResult<T> broadcastToAll(long timeout, boolean shutdown, Method method, int shardArrayParamIndex, Object... args) throws ChokException {
        return broadcastToAll(new ResultCompletePolicy<>(timeout, shutdown), method, shardArrayParamIndex, args);
    }

    public <T> ClientResult<T> broadcastToAll(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, Object... args) throws ChokException {
        return broadcastToShards(resultPolicy, method, shardArrayParamIndex, null, args);
    }

    public <T> ClientResult<T> broadcastToIndices(long timeout, boolean shutdown, Method method, int shardArrayIndex, String[] indices, Object... args) throws ChokException {
        return broadcastToIndices(new ResultCompletePolicy<>(timeout, shutdown), method, shardArrayIndex, indices, args);
    }

    public <T> ClientResult<T> broadcastToIndices(IResultPolicy<T> resultPolicy, Method method, int shardArrayIndex, String[] indices, Object... args) throws ChokException {
        if (indices == null) {
            indices = ALL_INDICES;
        }
        Map<String, List<String>> nodeShardsMap = getNode2ShardsMap(indices);
        if (nodeShardsMap.values().isEmpty()) {
            throw new ChokException("No shards for indices: " + (Arrays.asList(indices).toString()));
        }
        return broadcastInternal(resultPolicy, method, shardArrayIndex, nodeShardsMap, args);
    }

    public <T> ClientResult<T> singlecast(long timeout, boolean shutdown, Method method, int shardArrayParamIndex, String shard, Object... args) throws ChokException {
        return singlecast(new ResultCompletePolicy<>(timeout, shutdown), method, shardArrayParamIndex, shard, args);
    }

    public <T> ClientResult<T> singlecast(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, String shard, Object... args) throws ChokException {
        List<String> shards = Lists.newArrayList();
        shards.add(shard);
        return broadcastToShards(resultPolicy, method, shardArrayParamIndex, shards, args);
    }

    public <T> ClientResult<T> broadcastToShards(long timeout, boolean shutdown, Method method, int shardArrayParamIndex, Iterable<String> shards, Object... args) throws ChokException {
        return broadcastToShards(new ResultCompletePolicy<>(timeout, shutdown), method, shardArrayParamIndex, shards, args);
    }

    public <T> ClientResult<T> broadcastToShards(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, Iterable<String> shards, Object... args) throws ChokException {
        if (shards == null) {
            // If no shards specified, search all shards.
            shards = getAllShards();
        }
        final Map<String, List<String>> nodeShardsMap = selectionPolicy.createNode2ShardsMap(shards);
        if (nodeShardsMap.values().isEmpty()) {
            throw new ChokException("No shards selected: " + shards);
        }
        return broadcastInternal(resultPolicy, method, shardArrayParamIndex, nodeShardsMap, args);
    }

    private <T> ClientResult<T> broadcastInternal(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, Map<String, List<String>> nodeShardsMap, Object... args) {
        queryCount++;

        // Validate inputs.
        if (method == null || args == null) {
            throw new IllegalArgumentException("Null method or args!");
        }
        Class<?>[] types = method.getParameterTypes();

        validateArgs(types, args);
        validateShardArryaParamIndex(shardArrayParamIndex, types);

        if (LOG.isTraceEnabled()) {
            for (String indexName : indexToShards.keySet()) {
                LOG.trace("indexToShards " + indexName + " --> " + indexToShards.get(indexName).toString());
            }
            for (Map.Entry<String, List<String>> e : nodeShardsMap.entrySet()) {
                LOG.trace("broadcast using " + e.getKey() + " --> " + e.getValue().toString());
            }
            LOG.trace("selection policy = " + selectionPolicy);
        }

        // Make RPC calls to all nodes in parallel.
        long start = 0;
        if (LOG.isDebugEnabled()) {
            start = System.currentTimeMillis();
        }

        // We don't know what selectionPolicy built, and multiple threads may write
        // to map if IO errors occur. This map might be shared across multiple calls
        // also. So make a copy and synchronize it.
        Map<String, List<String>> nodeShardMapCopy = Maps.newHashMap();
        Set<String> allShards = Sets.newHashSet();
        for (Map.Entry<String, List<String>> e : nodeShardsMap.entrySet()) {
            nodeShardMapCopy.put(e.getKey(), Lists.newArrayList(e.getValue()));
            allShards.addAll(e.getValue());
        }
        nodeShardsMap = Collections.synchronizedMap(nodeShardMapCopy);
        //nodeShardMapCopy = null;

        WorkQueue<T> workQueue = new WorkQueue<>(proxyManager, allShards, method, shardArrayParamIndex, args);

        for (String node : nodeShardsMap.keySet()) {
            workQueue.execute(node, nodeShardsMap, 1, maxTryCount);
        }

        ClientResult<T> results = workQueue.getResults(resultPolicy);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("broadcast(%s(%s), %s) took %d msec for %s", method.getName(), args, nodeShardsMap, (System.currentTimeMillis() - start), results != null ? results : "null"));
        }
        return results;
    }

    private void validateShardArryaParamIndex(int shardArrayParamIndex, Class<?>[] types) {
        if (shardArrayParamIndex > 0) {
            if (shardArrayParamIndex >= types.length) {
                throw new IllegalArgumentException("shardArrayParamIndex out of range!");
            }
            if (!(types[shardArrayParamIndex]).equals(String[].class)) {
                throw new IllegalArgumentException("shardArrayParamIndex parameter (" + shardArrayParamIndex + ") is not of type String[]!");
            }
        }
    }

    private void validateArgs(Class<?>[] types, Object[] args) {
        if (args.length != types.length) {
            throw new IllegalArgumentException("Wrong number of args: found " + args.length + ", expected " + types.length + "!");
        }
        for (int i = 0; i < args.length; i++) {
            if (args[i] != null) {
                Class<?> from = args[i].getClass();
                Class<?> to = types[i];
                if (!to.isAssignableFrom(from) && !(from.isPrimitive() || to.isPrimitive())) {
                    // Assume autoboxing will work.
                    throw new IllegalArgumentException("Incorrect argument type for param " + i + ": expected " + types[i] + "!");
                }
            }
        }
    }

    // -------------------- Node management --------------------

    private Map<String, List<String>> getNode2ShardsMap(final String[] indexNames) throws ChokException {
        Collection<String> shardsToSearchIn = getShardsToSearchIn(indexNames);
        return selectionPolicy.createNode2ShardsMap(shardsToSearchIn);
    }

    private Collection<String> getShardsToSearchIn(String[] indexNames) throws ChokException {
        Collection<String> allShards = Sets.newHashSet();
        for (String index : indexNames) {
            if ("*".equals(index)) {
                ImmutableSet.copyOf(indexToShards.values()).forEach(allShards::add);
                break;
            }
            if (indexToShards.containsKey(index)) {
                allShards.addAll(indexToShards.get(index));
            } else {
                Pattern pattern = Pattern.compile(index);
                int matched = 0;
                for (String ind : ImmutableSet.copyOf(indexToShards.keySet())) {
                    if (pattern.matcher(ind).matches()) {
                        allShards.addAll(indexToShards.get(ind));
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

    public double getQueryPerMinute() {
        double minutes = (System.currentTimeMillis() - startupTime) / 60000.0;
        if (minutes > 0.0F) {
            return queryCount / minutes;
        }
        return 0.0F;
    }

    public void close() {
        if (protocol != null) {
            protocol.unregisterComponent(this);
            protocol.disconnect();
            protocol = null;
            proxyManager.shutdown();
        }
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

    public List<String> getIndices() {
        return ImmutableList.copyOf(indexToShards.keySet());
    }

    public boolean hasIndex(String index) {
        return indexToShards.containsKey(index);
    }

    public boolean hasShard(String index, String shardName) {
        return indexToShards.containsEntry(index, shardName);
    }

    private Iterable<String> getAllShards() {
        return indexToShards.values();
    }


}
