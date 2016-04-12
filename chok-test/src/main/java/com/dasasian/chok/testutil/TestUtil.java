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
package com.dasasian.chok.testutil;

import com.dasasian.chok.client.Client;
import com.dasasian.chok.master.Master;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.dasasian.chok.util.ZkConfiguration;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoAssertionError;
import org.mockito.stubbing.Stubber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class TestUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtil.class);

    /**
     * This waits until the provided {@link Callable} returns an object that is
     * equals to the given expected value or the timeout has been reached. In both
     * cases this method will return the return value of the latest
     * {@link Callable} execution.
     *
     * @param expectedValue The expected value of the callable.
     * @param callable      The callable.
     * @param <T>           The return type of the callable.
     * @param timeUnit      The timeout timeunit.
     * @param timeout       The timeout.
     * @return the return value of the latest {@link Callable} execution.
     * @throws Exception when an error occurs
     * @throws InterruptedException when interrupted
     */
    public static <T> T waitUntil(T expectedValue, Callable<T> callable, TimeUnit timeUnit, long timeout) throws Exception {
        long startTime = System.currentTimeMillis();
        do {
            T actual = callable.call();
            if (expectedValue.equals(actual)) {
                return actual;
            }
            if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
                return actual;
            }
            Thread.sleep(50);
        } while (true);
    }

    /**
     * This waits until a mockito verification passed (which is provided in the
     * runnable). This waits until the virification passed or the timeout has been
     * reached. If the timeout has been reached this method will rethrow the
     * {@link MockitoAssertionError} that comes from the mockito verification
     * code.
     *
     * @param runnable The runnable containing the mockito verification.
     * @param timeUnit The timeout timeunit.
     * @param timeout  The timeout.
     * @throws InterruptedException when interrupted
     */
    public static void waitUntilVerified(Runnable runnable, TimeUnit timeUnit, int timeout) throws InterruptedException {
        LOG.debug("Waiting for " + timeout + " " + timeUnit + " until verified.");
        long startTime = System.currentTimeMillis();
        do {
            MockitoAssertionError exception = null;
            try {
                runnable.run();
            } catch (MockitoAssertionError e) {
                exception = e;
            }
            if (exception == null) {
                return;
            }
            if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
                LOG.debug("Timeout reached without satifying expectations.");
                throw exception;
            }
            Thread.sleep(50);
        } while (true);
    }

    public static void waitUntilNoExceptionThrown(Runnable runnable, TimeUnit timeUnit, int timeout) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        do {
            RuntimeException exception = null;
            try {
                runnable.run();
            } catch (RuntimeException e) {
                exception = e;
            }
            if (exception == null) {
                return;
            }
            if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
                throw exception;
            }
            Thread.sleep(50);
        } while (true);
    }

    /**
     * Creates a Mockito answer object that can be used for asynchronously
     * stubbing. For example:
     * <br>
     * <pre>
     * final CountDownLatch countDownLatch = new CountDownLatch(1);
     * Mockito.doAnswer(TestUtil.createCountDownAnswer(countDownLatch)).when(listener).announceNode(nodeName);
     * mock.someMethod(someValue);
     * countDownLatch.await(10, TimeUnit.SECONDS);
     * Assert.assertEquals(&quot;expecting invocation within 10 seconds&quot;, 0, countDownLatch.getCount());
     * </pre>
     * @param countDownLatch the countdown latch
     * @return the stubber
     */

    public static Stubber createCountDownAnswer(final CountDownLatch countDownLatch) {
        return Mockito.doAnswer(invocation -> {
            countDownLatch.countDown();
            return null;
        });
    }

    public static boolean indexHasDeployError(final InteractionProtocol protocol, final String indexName) throws Exception {
        IndexMetaData indexMetaData = protocol.getIndexMD(indexName);
        return indexMetaData.hasDeployError();
    }

    public static void waitUntilLeaveSafeMode(final Master master) throws Exception {
        waitUntil(false, master::isInSafeMode, TimeUnit.SECONDS, 30);
        assertEquals(false, master.isInSafeMode());
    }

    public static void waitUntilBecomeMaster(final Master master) throws Exception {
        waitUntil(false, () -> !master.isMaster(), TimeUnit.SECONDS, 30);
        assertEquals(true, master.isMaster());
    }

    public static void waitUntilIndexDeployed(final InteractionProtocol protocol, final String indexName) throws Exception {
        waitUntil(false, () -> protocol.getIndexMD(indexName) == null, TimeUnit.SECONDS, 30);
    }

    public static void waitUntilIndexBalanced(final InteractionProtocol protocol, final String indexName) throws Exception {
        waitUntil(true, () -> {
            IndexMetaData indexMD = protocol.getIndexMD(indexName);
            if (indexMD.hasDeployError()) {
                throw new IllegalStateException("index " + indexName + " has a deploy error");
            }
            return protocol.getReplicationReport(indexMD, protocol.getLiveNodeCount()).isBalanced();
        }, TimeUnit.SECONDS, 30);
    }

    public static void waitUntilShardsUndeployed(final InteractionProtocol protocol, final IndexMetaData indexMD) throws Exception {
        TestUtil.waitUntil(false, () -> {
            int nodeCount = 0;
            Set<Shard> shards = indexMD.getShards();
            for (Shard shard : shards) {
                try {
                    nodeCount += protocol.getShardNodes(shard.getName()).size();
                } catch (ZkNoNodeException e) {
                    // deleted already
                }
            }
            return nodeCount != 0;
        }, TimeUnit.SECONDS, 30);
    }

    public static void waitUntilNodeServesShards(final InteractionProtocol protocol, final String nodeName, final int shardCount) throws Exception {
        waitUntil(true, () -> protocol.getNodeShards(nodeName).size() == shardCount, TimeUnit.SECONDS, 30);
        assertEquals(shardCount, protocol.getNodeShards(nodeName).size());
    }

    public static void waitUntilNumberOfLiveNode(final InteractionProtocol protocol, final int nodeCount) throws Exception {
        waitUntil(true, () -> protocol.getLiveNodeCount() == nodeCount, TimeUnit.SECONDS, 30);
        assertEquals(nodeCount, protocol.getLiveNodeCount());
    }

    public static void waitUntilEmptyOperationQueues(final InteractionProtocol protocol, final Master master, final List<Node> nodes) throws Exception {
        waitUntil(true, () -> operationQueuesEmpty(protocol, master, nodes), TimeUnit.SECONDS, 30);
        assertThat(operationQueuesEmpty(protocol, master, nodes)).as("node operation queues are empty").isTrue();
    }

    private static Boolean operationQueuesEmpty(final InteractionProtocol protocol, Master master, final List<Node> nodes) {
        ZkConfiguration zkConf = protocol.getZkConfiguration();
        if (protocol.getZkClient().countChildren(zkConf.getPath(PathDef.MASTER_QUEUE, "operations")) > 0) {
            return false;
        }
        for (Node node : nodes) {
            if (protocol.getZkClient().countChildren(zkConf.getPath(PathDef.NODE_QUEUE, node.getName(), "operations")) > 0) {
                return false;
            }
        }

        return true;
    }

    public static void waitUntilClientHasIndex(Client client, String indexName) throws InterruptedException {
        while (!client.hasIndex(indexName)) {
            Thread.sleep(10);
        }
    }
}
