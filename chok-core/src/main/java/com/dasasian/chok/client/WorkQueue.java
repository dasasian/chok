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

import com.google.common.collect.ImmutableSetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * This class manages the multiple NodeInteraction threads for a call.
 * The initial node interactions and any resulting retries go through the
 * same execute() method. We allow blocking or non-blocking access
 * to the result set, or you can provide a custom policy to control the
 * length of time spent waiting for results to complete.
 */
class WorkQueue<T> implements INodeExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(WorkQueue.class);

    private static int instanceCounter = 0;
    private final INodeInteractionFactory<T> interactionFactory;
    private final INodeProxyManager shardManager;
    private final Method method;
    private final int shardArrayParamIndex;
    private final Object[] args;
    private final ExecutorService executor;
    private final IResultReceiverWrapper<T> resultReceiverWrapper;
    private final int instanceId = instanceCounter++;
    private int callCounter = 0;

    /**
     * Used by unit tests to make toString() output repeatable.
     */
    public static void resetInstanceCounter() {
        instanceCounter = 0;
    }

    /**
     * Normal constructor. Jobs submitted by execute() will result in a
     * NodeInteraction instance being created and run(). The WorkQueue
     * is initially emtpy. Call execute() to add jobs.
     * <p>
     * <b>DO NOT CHANGE THE ARGUMENTS WHILE THIS CALL IS RUNNING OR YOU WILL BE
     * SORRY.</b>
     *
     * @param shardManager         The class that maintains the node/shard maps, the node selection
     *                             policy, and the node proxies.
     * @param method               Which method to call on the server side.
     * @param shardArrayParamIndex Which paramater, if any, should be overwritten with an array of
     *                             the shard names (per server call). Pass -1 to disable this.
     * @param args                 The arguments to pass in to the method on the server side.
     */
    protected WorkQueue(ExecutorService executor, INodeProxyManager shardManager, Method method, int shardArrayParamIndex, IResultReceiverWrapper<T> resultReceiverWrapper, Object... args) {
        this(executor, NodeInteraction::new, shardManager, method, shardArrayParamIndex, resultReceiverWrapper, args);
    }

    /**
     * Used by unit tests. By providing an alternate factory, this class can be tested without creating
     * and NodeInteractions.
     *  @param executor             The executor to use for the queue
     * @param shardManager         The class that maintains the node/shard maps, the node selection
     *                             policy, and the node proxies.
     * @param method               Which method to call on the server side.
     * @param shardArrayParamIndex Which paramater, if any, should be overwritten with an array of
     *                             the shard names (per server call). Pass -1 to disable this.
     * @param args                 The arguments to pass in to the method on the server side.
     */
    protected WorkQueue(ExecutorService executor, INodeInteractionFactory<T> interactionFactory, INodeProxyManager shardManager, Method method, int shardArrayParamIndex, IResultReceiverWrapper<T> resultReceiverWrapper, Object... args) {
        this.executor = executor;
        if (shardManager == null || method == null) {
            throw new IllegalArgumentException("Null passed to new WorkQueue()");
        }
        this.interactionFactory = interactionFactory;
        this.shardManager = shardManager;
        this.method = method;
        this.shardArrayParamIndex = shardArrayParamIndex;
        this.args = args != null ? args : new Object[0];
        this.resultReceiverWrapper = resultReceiverWrapper;
        if (LOG.isTraceEnabled()) {
            LOG.trace("Creating new " + this);
        }
    }

    /**
     * Submit a job, which is a call to a server node via an RPC proxy using a NodeInteraction.
     * Ignored if called after shutdown(), or after result set is closed.
     *
     * @param node         The node on which to execute the method.
     * @param nodeShardMap The current node shard map, with failed nodes removed if this is a retry.
     * @param tryCount     This call is the Nth retry. Starts at 1.
     * @param maxTryCount  How often the call should be repeated in maximum.
     */
    public void execute(String node, ImmutableSetMultimap<String, String> nodeShardMap, int tryCount, int maxTryCount) {
        if (!executor.isShutdown() && !resultReceiverWrapper.isClosed()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("Creating interaction with %s, will use shards: %s, tryCount=%d (id=%d)", node, nodeShardMap.get(node), tryCount, instanceId));
            }
            Runnable interaction = interactionFactory.createInteraction(method, args, shardArrayParamIndex, node, nodeShardMap, tryCount, maxTryCount, shardManager, this, resultReceiverWrapper);
            if (interaction != null) {
                try {
                    executor.execute(interaction);
                } catch (RejectedExecutionException e) {
                    // This could happen, but should be rare.
                    LOG.warn(String.format("Failed to submit node interaction %s (id=%d)", interaction, instanceId));
                }
            } else {
                LOG.error("Null node interaction runnable for node " + node);
            }
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("Not creating interaction with %s, shards=%s, tryCount=%d, executor=%s, result=%s (id=%d)", node, nodeShardMap.get(node), tryCount, executor.isShutdown() ? "shutdown" : "running", resultReceiverWrapper, instanceId));
            }
        }
    }

    /**
     * Use a user-provided policy to decide how long to wait for and whether to
     * terminate the call.
     *
     * @param policy How to decide when to return and to terminate the call.
     */
    public void waitTillDone(IResultPolicy<T> policy) throws Exception {
        int callId = callCounter++;
        long start = 0;
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("getResults() policy = %s (id=%d:%d)", policy, instanceId, callId));
            start = System.currentTimeMillis();
        }
        long waitTime;
        while (true) {
            synchronized (resultReceiverWrapper) {
                // Need to stay synchronized before waitTime() through wait() or we will
                // miss notifications.
                waitTime = policy.waitTime(resultReceiverWrapper);
                if (waitTime > 0 && !resultReceiverWrapper.isClosed()) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("Waiting %d ms, results = %s (id=%d:%d)", waitTime, resultReceiverWrapper, instanceId, callId));
                    }
                    try {
                        resultReceiverWrapper.wait(waitTime);
                    }
                    catch (InterruptedException e) {
                        LOG.debug("Interrupted", e);
                    }
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("Done waiting, results = %s (id=%d:%d)", resultReceiverWrapper, instanceId, callId));
                    }
                }
                else {
                    break;
                }
            }
        }
//        if (waitTime < 0) {
//            if (LOG.isTraceEnabled()) {
//                LOG.trace(String.format("Shutting down work queue, results = %s (id=%d:%d)", resultReceiverWrapper, instanceId, callId));
//            }
//            resultReceiverWrapper.close();
//        }
//        if (executor.isShutdown()) {
//            if (LOG.isTraceEnabled()) {
//                LOG.trace(String.format("Executor is shutdown. Shutting down work queue, results = %s (id=%d:%d)", resultReceiverWrapper, instanceId, callId));
//            }
//            resultReceiverWrapper.close();
//        }
        if (LOG.isTraceEnabled()) {
            long time = System.currentTimeMillis() - start;
            LOG.trace(String.format("Returning results = %s, took %d ms (id=%d:%d)", resultReceiverWrapper, time, instanceId, callId));
        }
    }

    @Override
    public String toString() {
        String argsStr = Arrays.asList(args).toString();
        argsStr = argsStr.substring(1, argsStr.length() - 1);
        return String.format("WorkQueue[%s.%s(%s) (id=%d)]", method.getDeclaringClass().getSimpleName(), method.getName(), argsStr, instanceId);
    }

    public interface INodeInteractionFactory<T> {
        Runnable createInteraction(Method method, Object[] args, int shardArrayParamIndex, String node, ImmutableSetMultimap<String, String> nodeShardMap, int tryCount, int maxTryCount, INodeProxyManager shardManager, INodeExecutor nodeExecutor, IResultReceiverWrapper<T> results);
    }

}
