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

import com.dasasian.chok.util.ChokException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Client helper
 *
 * Created by damith.chandrasekara on 8/26/15.
 */
public class ClientHelper {

    private final static Logger LOG = LoggerFactory.getLogger(ClientHelper.class);

    private long queryCount = 0;
    private final long startupTime;
    private final int maxTryCount;
    private final SetMultimap<String, String> indexToShards;
    private final INodeProxyManager proxyManager;
    private final INodeSelectionPolicy selectionPolicy;

    public ClientHelper(int maxTryCount, SetMultimap<String, String> indexToShards,INodeProxyManager proxyManager, INodeSelectionPolicy selectionPolicy) {
        this.maxTryCount = maxTryCount;
        this.indexToShards = indexToShards;
        this.proxyManager = proxyManager;
        this.selectionPolicy = selectionPolicy;
        startupTime = System.currentTimeMillis();
    }


    public <T> void broadcastInternalReceiver(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, ImmutableSetMultimap<String, String> nodeShardsMap, IResultReceiver<T> resultReceiver, Object... args) throws ChokException {
        final ExecutorService executorService = Executors.newCachedThreadPool();

        try {
            try (ResultReceiverWrapper<T> resultReceiverWrapper = new ResultReceiverWrapper<>(ImmutableSet.copyOf(nodeShardsMap.values()), resultReceiver)) {
                broadcastInternalReceiverWrapper(executorService, resultPolicy, method, shardArrayParamIndex, nodeShardsMap, resultReceiverWrapper, args);
            }
        }
        finally {
            if(!executorService.isShutdown()) {
                executorService.shutdownNow();
            }
        }
    }

    protected  <T> void broadcastInternalReceiverWrapper(ExecutorService executor, IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, ImmutableSetMultimap<String, String> node2ShardsMap, IResultReceiverWrapper<T> resultReceiverWrapper, Object... args) throws ChokException {
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
                LOG.trace("index2Shards " + indexName + " --> " + indexToShards.get(indexName).toString());
            }
            for (Map.Entry<String, Collection<String>> e : node2ShardsMap.asMap().entrySet()) {
                LOG.trace("broadcast using " + e.getKey() + " --> " + e.getValue().toString());
            }
            LOG.trace("selection policy = " + selectionPolicy);
        }

        // Make RPC calls to all nodes in parallel.
        long start = 0;
        if (LOG.isDebugEnabled()) {
            start = System.currentTimeMillis();
        }

        try {
            WorkQueue<T> workQueue = new WorkQueue<>(executor, proxyManager, method, shardArrayParamIndex, resultReceiverWrapper, args);
            node2ShardsMap.keySet().stream().forEach(node -> workQueue.execute(node, node2ShardsMap, 1, maxTryCount));
            workQueue.waitTillDone(resultPolicy);
        }
        catch (Exception e) {
            throw new ChokException("Got error", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("broadcast(%s(%s), %s) took %d msec", method.getName(), Arrays.toString(args), node2ShardsMap, (System.currentTimeMillis() - start)));
        }
    }

    public <T> ClientResult<T> broadcastInternal(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex, ImmutableSetMultimap<String, String> nodeShardsMap, Object... args) throws ChokException {
        final ExecutorService executorService = Executors.newCachedThreadPool();

        try (ClientResult<T> clientResults = new ClientResult<>(ImmutableSet.copyOf(nodeShardsMap.values()))){
            broadcastInternalReceiverWrapper(executorService, resultPolicy, method, shardArrayParamIndex, nodeShardsMap, clientResults.getResultReceiverWrapper(), args);
            return clientResults;
        }
        finally {
            if(!executorService.isShutdown()) {
                executorService.shutdownNow();
            }
        }
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

    public double getQueryPerMinute() {
        double minutes = (System.currentTimeMillis() - startupTime) / 60000.0;
        if (minutes > 0.0F) {
            return queryCount / minutes;
        }
        return 0.0F;
    }

}
