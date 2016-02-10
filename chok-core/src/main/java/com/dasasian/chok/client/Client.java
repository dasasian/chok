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

import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.*;
import com.google.common.collect.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;

public class Client implements AutoCloseable {

    private final static Logger LOG = LoggerFactory.getLogger(Client.class);

    private static final String[] ALL_INDICES = new String[]{"*"};

    // todo should this move to IndexAddRemoveListener?
    private final INodeSelectionPolicy selectionPolicy;
    private final ClientHelper clientHelper;
    private final InteractionProtocol protocol;
    private final INodeProxyManager proxyManager;
    private final IndexAddRemoveListener indexAddRemoveListener;

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
        this(policy, protocol, getProxyManager(serverClass, policy, clientConfiguration), clientConfiguration.getInt(ClientConfiguration.CLIENT_NODE_INTERACTION_MAXTRYCOUNT));
    }

    protected static INodeProxyManager getProxyManager(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy policy, ClientConfiguration clientConfiguration) {
        return new NodeProxyManager(serverClass, getHadoopConfiguration(clientConfiguration), policy);
    }

    protected static Configuration getHadoopConfiguration(ClientConfiguration clientConfiguration) {
        Set<String> keys = ImmutableSet.copyOf(clientConfiguration.getKeys());
        Configuration hadoopConf = new Configuration();
        synchronized (Configuration.class) {
            for (String key : keys) {
                // simply set all properties / adding non-hadoop properties shouldn't hurt
                hadoopConf.set(key, clientConfiguration.getProperty(key));
            }
        }
        return hadoopConf;
    }

    protected Client(final INodeSelectionPolicy policy, final InteractionProtocol protocol, final INodeProxyManager proxyManager, int maxTryCount) {
        this.proxyManager = proxyManager;
        this.selectionPolicy = policy;
        this.protocol = protocol;
        this.indexAddRemoveListener = new IndexAddRemoveListener(policy, protocol, proxyManager);
        this.clientHelper = new ClientHelper(maxTryCount, indexAddRemoveListener.getIndex2Shards(), proxyManager, policy);
    }


    public INodeSelectionPolicy getSelectionPolicy() {
        return selectionPolicy;
    }

    public INodeProxyManager getProxyManager() {
        return proxyManager;
    }

    // --------------- Distributed calls to servers ----------------------

    /**
     * Broadcast a method call to all indices. Return all the results in a
     * Collection.
     *
     * @param <T>                  the class for the result
     * @param resultPolicy         policy to wait on results
     * @param method               The server's method to call.
     * @param shardArrayParamIndex Which parameter of the method call, if any, that should be
     *                             replaced with the shards to search. This is an array of Strings,
     *                             with a different value for each node / server. Pass in -1 to
     *                             disable.
     * @param args                 The arguments to pass to the method when run on the server.
     * @return the results
     * @throws ChokException on exception
     */
    @SuppressWarnings("unused")
    public <T> ClientResult<T> broadcastToAll(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, Object... args) throws ChokException {
        return broadcastToShards(resultPolicy, method, shardArrayParamIndex, null, args);
    }

    /**
     * Broadcast request to indices.
     *
     * @param resultPolicy the policy to use for waiting for the result
     * @param method the method to broadcast
     * @param shardArrayParamIndex Which parameter of the method call, if any, that should be
     *                             replaced with the shards to search. This is an array of Strings,
     *                             with a different value for each node / server. Pass in -1 to
     *                             disable.
     * @param indices the indices to query
     * @param args the method arguments
     * @param <T> the result type
     * @return a client result containing the results from all the nodes
     * @throws ChokException when an error occurs
     */
    @SuppressWarnings("unused")
    public <T> ClientResult<T> broadcastToIndices(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, String[] indices, Object... args) throws ChokException {
        if (indices == null) {
            indices = ALL_INDICES;
        }
        ImmutableSetMultimap<String, String> nodeShardsMap = indexAddRemoveListener.getNode2ShardsMap(indices);
        if (nodeShardsMap.values().isEmpty()) {
            throw new ChokException("No shards for indices: " + (Arrays.asList(indices).toString()));
        }
        return clientHelper.broadcastInternal(resultPolicy, method, shardArrayParamIndex, nodeShardsMap, args);
    }

    /**
     * Broadcast request to single shard.
     *
     * @param resultPolicy the policy to use for waiting for the result
     * @param method the method to broadcast
     * @param shardArrayParamIndex Which parameter of the method call, if any, that should be
     *                             replaced with the shards to search. This is an array of Strings,
     *                             with a different value for each node / server. Pass in -1 to
     *                             disable.
     * @param shard the shard to query
     * @param args the method arguments
     * @param <T> the result type
     * @return a client result containing the results from all the nodes
     * @throws ChokException when an error occurs
     */
    @SuppressWarnings("unused")
    public <T> ClientResult<T> singlecast(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, String shard, Object... args) throws ChokException {
        List<String> shards = Lists.newArrayList();
        shards.add(shard);
        return broadcastToShards(resultPolicy, method, shardArrayParamIndex, shards, args);
    }

    /**
     * Broadcast request to shards.
     *
     * @param resultPolicy the policy to use for waiting for the result
     * @param method the method to broadcast
     * @param shardArrayParamIndex Which parameter of the method call, if any, that should be
     *                             replaced with the shards to search. This is an array of Strings,
     *                             with a different value for each node / server. Pass in -1 to
     *                             disable.
     * @param shards the shards to query
     * @param args the method arguments
     * @param <T> the result type
     * @return a client result containing the results from all the nodes
     * @throws ChokException when an error occurs
     */
    @SuppressWarnings("unused")
    public <T> ClientResult<T> broadcastToShards(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, Iterable<String> shards, Object... args) throws ChokException {
        if (shards == null) {
            // If no shards specified, search all shards.
            shards = getAllShards();
        }
        final ImmutableSetMultimap<String, String> nodeShardsMap = selectionPolicy.createNode2ShardsMap(shards);
        if (nodeShardsMap.values().isEmpty()) {
            throw new ChokException("No shards selected: " + shards);
        }
        return clientHelper.broadcastInternal(resultPolicy, method, shardArrayParamIndex, nodeShardsMap, args);
    }

    /**
     * Broadcast request to shards with receiver.
     *
     * @param resultPolicy the policy to use for waiting for the result
     * @param method the method to broadcast
     * @param shardArrayParamIndex Which parameter of the method call, if any, that should be
     *                             replaced with the shards to search. This is an array of Strings,
     *                             with a different value for each node / server. Pass in -1 to
     *                             disable.
     * @param shards the shards to query
     * @param resultReceiver the result receiver
     * @param args the method arguments
     * @param <T> the result type
     * @throws ChokException when an error occurs
     */
    @SuppressWarnings("unused")
    public <T> void broadcastToShardsReceiver(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, Iterable<String> shards, IResultReceiver<T> resultReceiver, Object... args) throws ChokException {
        if (shards == null) {
            // If no shards specified, search all shards.
            shards = getAllShards();
        }
        final ImmutableSetMultimap<String, String> nodeShardsMap = selectionPolicy.createNode2ShardsMap(shards);
        if (nodeShardsMap.values().isEmpty()) {
            throw new ChokException("No shards selected: " + shards);
        }
        clientHelper.broadcastInternalReceiver(resultPolicy, method, shardArrayParamIndex, nodeShardsMap, resultReceiver, args);
    }


    // -------------------- Node management --------------------

    public double getQueryPerMinute() {
        return clientHelper.getQueryPerMinute();
    }

    public void close() {
        indexAddRemoveListener.close();

            protocol.disconnect();
            proxyManager.shutdown();
    }

    @SuppressWarnings("unused")
    public List<String> getIndices() {
        return ImmutableList.copyOf(indexAddRemoveListener.getIndex2Shards().keySet());
    }

    @SuppressWarnings("unused")
    public boolean hasIndex(String index) {
        return indexAddRemoveListener.getIndex2Shards().containsKey(index);
    }

    @SuppressWarnings("unused")
    public boolean hasShard(String index, String shardName) {
        return indexAddRemoveListener.getIndex2Shards().containsEntry(index, shardName);
    }

    @SuppressWarnings("unused")
    private Collection<String> getAllShards() {
        return Collections.unmodifiableCollection(indexAddRemoveListener.getIndex2Shards().values());
    }


}
