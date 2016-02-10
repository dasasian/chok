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

import com.dasasian.chok.client.WorkQueue.INodeInteractionFactory;
import com.dasasian.chok.testutil.AbstractTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * Test for {@link WorkQueue}.
 */
public class WorkQueueTest extends AbstractTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(WorkQueueTest.class);

    /**
     * Returns an interaction factory that ignores all calls and does nothing.
     */
    public static <T> INodeInteractionFactory<T> nullFactory() {
        return (method, args, shardArrayParamIndex, node, nodeShardMap, tryCount, maxTryCount, shardManager, nodeExecutor, results) -> null;
    }

    protected static void sleep(long msec) {
        long now = System.currentTimeMillis();
        long stop = now + msec;
        while (now < stop) {
            try {
                Thread.sleep(stop - now);
            } catch (InterruptedException e) {
                // proceed
            }
            now = System.currentTimeMillis();
        }
    }

    private ExecutorService executorService;
    private TestNodeInteractionFactory factory;
    private TestShardManager shardManager;


    @Before
    public void setUp() {
        executorService = Executors.newCachedThreadPool();
        factory = new TestNodeInteractionFactory(10);
        shardManager = new TestShardManager();
        WorkQueue.resetInstanceCounter();
    }

    @After
    public void tearDown() {
        if (!executorService.isShutdown()) {
            executorService.shutdown();
        }
        factory = null;
        shardManager = null;
    }

    @Test
    public void testWorkQueue() throws Exception {
        Method method = TestServer.class.getMethod("doSomething", Integer.TYPE);

        for (int i = 0; i < 500; i++) {
            shardManager.reset();
            TestNodeInteractionFactory factory = new TestNodeInteractionFactory(10);
            ImmutableSetMultimap<String, String> node2ShardsMap = shardManager.createNode2ShardsMap(shardManager.allShards());


            ClientResult<Integer> result = new ClientResult<>(shardManager.allShards());
            WorkQueue<Integer> wq = new WorkQueue<>(executorService, factory, shardManager, method, -1, result.getResultReceiverWrapper(), 16);
            assertEquals(String.format("WorkQueue[TestServer.doSomething(16) (id=%d)]", i), wq.toString());
            for (String node : node2ShardsMap.keySet()) {
                wq.execute(node, node2ShardsMap, 1, 3);
            }
            wq.waitTillDone(ResultCompletePolicy.awaitCompletion(100000));

            result.close();

            int numNodes = node2ShardsMap.keySet().size();
            int numShards = shardManager.allShards().size();
            assertEquals(String.format("ClientResult: %d results, 0 errors, %d/%d shards (closed) (complete)", numNodes, numShards, numShards), result.toString());
            assertEquals(6, factory.getCalls().size());
        }
    }

    @Test
    public void testSubmitAfterShutdown() throws Exception {
        ImmutableSet<String> allShards = shardManager.allShards();
        ImmutableSetMultimap<String, String> node2ShardsMap = shardManager.createNode2ShardsMap(allShards);
        Method method = TestServer.class.getMethod("doSomething", Integer.TYPE);

        try (ClientResult<Integer> result = new ClientResult<>(shardManager.allShards())) {
            WorkQueue<Integer> wq = new WorkQueue<>(executorService, factory, shardManager, method, -1, result.getResultReceiverWrapper(), 16);
//            for (String node : node2ShardsMap.keySet()) {
//                wq.execute(node, node2ShardsMap, 1, 3);
//            }
            wq.waitTillDone(ResultCompletePolicy.awaitCompletion(0));
            assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", result.toString());
            executorService.shutdown();
            wq.waitTillDone(ResultCompletePolicy.awaitCompletion(0));
            assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", result.toString());
            for (String node : node2ShardsMap.keySet()) {
                wq.execute(node, node2ShardsMap, 1, 3);
            }
            assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", result.toString());
        }
        assertThat(factory.getCalls().size(), is(equalTo(0)));
    }

    @Test
    public void testSubmitAfterClose() throws Exception {
        ImmutableSet<String> allShards = shardManager.allShards();
        ImmutableSetMultimap<String, String> node2ShardsMap = shardManager.createNode2ShardsMap(allShards);
        Method method = TestServer.class.getMethod("doSomething", Integer.TYPE);

        try (ClientResult<Integer> result = new ClientResult<>(shardManager.allShards())) {
            WorkQueue<Integer> wq = new WorkQueue<>(executorService, factory, shardManager, method, -1, result.getResultReceiverWrapper(), 16);
////            for (String node : node2ShardsMap.keySet()) {
////                wq.execute(node, node2ShardsMap, 1, 3);
////            }
//            wq.waitTillDone(0, false);
            assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", result.toString());
            result.close();
            assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards (closed)", result.toString());
            for (String node : node2ShardsMap.keySet()) {
                wq.execute(node, node2ShardsMap, 1, 3);
            }
            assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards (closed)", result.toString());
        }
        assertThat(factory.getCalls().size(), is(equalTo(0)));
    }

    @Test
    public void testGetResultTimeout() throws Exception {
        factory.additionalSleepTime = 60000;
        ImmutableSet<String> allShards = shardManager.allShards();
        ImmutableSetMultimap<String, String> node2ShardsMap = shardManager.createNode2ShardsMap(allShards);
        Method method = TestServer.class.getMethod("doSomething", Integer.TYPE);

        try(ClientResult<Integer> result = new ClientResult<>(shardManager.allShards())) {
            WorkQueue<Integer> wq = new WorkQueue<>(executorService, factory, shardManager, method, -1, result.getResultReceiverWrapper(), 16);
            for (String node : node2ShardsMap.keySet()) {
                wq.execute(node, node2ShardsMap, 1, 3);
            }

            int numShards = allShards.size();
            long slop = 20;
            // No delay
            long t1 = System.currentTimeMillis();
            wq.waitTillDone(ResultCompletePolicy.awaitCompletion(0));
            long t2 = System.currentTimeMillis();
            assertEquals(String.format("ClientResult: 0 results, 0 errors, 0/%d shards", numShards), result.toString());
            assertTrue(t2 - t1 < slop);
            // Short delay
            t1 = System.currentTimeMillis();
            wq.waitTillDone(ResultCompletePolicy.awaitCompletion(500));
            t2 = System.currentTimeMillis();
            assertEquals(String.format("ClientResult: 0 results, 0 errors, 0/%d shards", numShards), result.toString());
            assertTrue(t2 - t1 >= 500);
            assertTrue(t2 - t1 < 500 + slop);
            // Tiny delay.
            t1 = System.currentTimeMillis();
            wq.waitTillDone(ResultCompletePolicy.awaitCompletion(10));
            t2 = System.currentTimeMillis();
            assertEquals(String.format("ClientResult: 0 results, 0 errors, 0/%d shards", numShards), result.toString());
            assertTrue(t2 - t1 >= 10);
            assertTrue(t2 - t1 < 10 + slop);
            // Stop soon.
            t1 = System.currentTimeMillis();
            wq.waitTillDone(ResultCompletePolicy.awaitCompletion(100));
            t2 = System.currentTimeMillis();
            assertEquals(String.format("ClientResult: 0 results, 0 errors, 0/%d shards", numShards), result.toString());
            assertTrue(t2 - t1 >= 100);
            assertTrue(t2 - t1 < 100 + slop);
        }
    }

    // Does user calling close() wake up the work queue?
    @Test
    public void testUserCloseEvent() throws Exception {
        INodeInteractionFactory<String> factory = nullFactory();
        Method method = Object.class.getMethod("toString");

        for (IResultPolicy<String> policy : ImmutableList.of(
//                ResultCompletePolicy.<String>awaitCompletionThenShutdown(4000),
//                ResultCompletePolicy.<String>awaitCompletionThenShutdown(4000),
                ResultCompletePolicy.<String>awaitCompletion(4000),
//                ResultCompletePolicy.<String>awaitCompletion(50, 950, 0.99),
//                ResultCompletePolicy.<String>awaitCompletion(950, 50, 0.99),
                ResultCompletePolicy.<String>awaitCompletion(50, 950, 0.99),
                ResultCompletePolicy.<String>awaitCompletion(950, 50, 0.99))) {

            try (final ClientResult<String> result = new ClientResult<>(shardManager.allShards())) {
                final WorkQueue<String> wq = new WorkQueue<>(executorService, factory, shardManager, method, -1, result.getResultReceiverWrapper(), 16);
                wq.waitTillDone(ResultCompletePolicy.awaitCompletion(0));
                assertThat(result.isClosed(), is(false));
                sleep(10);

                // Simulate the user polling then eventually closing the result.
                final long start = System.currentTimeMillis();
                new Thread(() -> {
                    sleep(100);
                    result.close();
                }).start();

                // Now block on results.
                wq.waitTillDone(policy);
                long time = System.currentTimeMillis() - start;
                //
                if (time <= 50 || time >= 200) {
                    System.err.println("Took " + time + ", expected 100. Policy = " + policy);
                }
                assertTrue(time > 50);
                assertTrue(time < 200);
                assertTrue(result.isClosed());
            }
        }
    }

    // Does IResultPolicy calling close() wake up the work queue?
    @Test(timeout = 10000)
    public void testPolicyCloseEvent() throws Exception {
        INodeInteractionFactory<String> factory = nullFactory();
        ImmutableSet<String> allShards = shardManager.allShards();
        Method method = Object.class.getMethod("toString");

        IResultPolicy<String> policy = new IResultPolicy<String>() {
            private long now = System.currentTimeMillis();
            private long closeTime = now + 100;
            private long stopTime = now + 1000;

            @Override
            public long waitTime(IResultReceiverWrapper<String> result) {
                final long innerNow = System.currentTimeMillis();
                if (innerNow >= closeTime) {
                    try {
                        result.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                if (innerNow >= stopTime) {
                    return 0;
                } else if (innerNow >= closeTime) {
                    return stopTime - innerNow;
                } else {
                    return closeTime - innerNow;
                }
            }
        };

        try (ClientResult<String> result = new ClientResult<>(allShards)) {
            WorkQueue<String> wq = new WorkQueue<>(executorService, factory, shardManager, method, -1, result.getResultReceiverWrapper(), 16);
            sleep(10);

            long startTime = System.currentTimeMillis();
            wq.waitTillDone(policy);
            long time = System.currentTimeMillis() - startTime;
            assertTrue(result.isClosed());
            //
            if (time <= 50 || time >= 200) {
                System.err.println("Took " + time + ", expected 100. Policy = " + policy);
            }
            assertTrue(time > 50);
            assertTrue(time < 200);
        }
    }

    @Test
    public void testPolling() throws Exception {
        shardManager = new TestShardManager(null, 80, 1);
        factory = new TestNodeInteractionFactory(2500);
        Method method = TestServer.class.getMethod("doSomething", Integer.TYPE);
        ImmutableSet<String> allShards = shardManager.allShards();
        ImmutableSetMultimap<String, String> node2ShardsMap = shardManager.createNode2ShardsMap(allShards);

        try (ClientResult<Integer> result = new ClientResult<>(allShards)) {
            WorkQueue<Integer> wq = new WorkQueue<>(executorService, factory, shardManager, method, -1, result.getResultReceiverWrapper(), 16);
//            for (String node : node2ShardsMap.keySet()) {
//                wq.execute(node, node2ShardsMap, 1, 3);
//            }
//            wq.waitTillDone(0, false);
            System.out.println("Expected graph:");
            for (int len : new int[]{0, 6, 12, 16, 23, 34, 40, 50, 51, 58, 64, 68, 76, 80}) {
                bar(len);
            }
            System.out.println("Progress:");
            for (String node : node2ShardsMap.keySet()) {
                wq.execute(node, node2ShardsMap, 1, 3);
            }
            double coverage;
            do {
                coverage = result.getShardCoverage();
                int len = (int) Math.round(coverage * 80);
                bar(len);
                if (coverage < 1.0) {
                    sleep(200);
                }
            } while (coverage < 1.0);
            System.out.println("Done.");
        }
    }

    private void bar(int len) {
        StringBuilder sb = new StringBuilder();
        sb.append('|');
        for (int i = 0; i < 80; i++) {
            sb.append(i < len ? '#' : ' ');
        }
        sb.append('|');
        System.out.println(sb);
    }

    public interface ProxyProvider {
        VersionedProtocol getProxy(String node);
    }

    public static class TestShardManager implements INodeProxyManager {
        private int numNodes;
        private int replication;
        private ImmutableSet<String> allShards;
        private ImmutableSetMultimap<String, String> shardMap;
        private INodeSelectionPolicy nodeSelectionPolicy;
        private ProxyProvider proxyProvider;
        private boolean shardMapsFail = false;

        public TestShardManager() {
            this(null, 8, 3);
        }

        public TestShardManager(ProxyProvider proxyProvider, int numNodes, int replication) {
            this.proxyProvider = proxyProvider;
            this.numNodes = numNodes;
            this.replication = replication;
            reset();
        }

        public void reset() {
            // Nodes n1, n2, n3...
            String[] nodes = new String[numNodes];
            for (int i = 0; i < numNodes; i++) {
                nodes[i] = "n" + (i + 1);
            }
            // Shards s1, s3, s3... (same # as nodes)
            String[] shards = new String[numNodes];
            for (int i = 0; i < numNodes; i++) {
                shards[i] = "s" + (i + 1);
            }
            allShards = ImmutableSet.copyOf(shards);
            // Node i has shards i, i+1, i+2... depending on replication level.
            ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();
            for (int i = 0; i < numNodes; i++) {
                for (int j = 0; j < replication; j++) {
                    builder.put(nodes[i], shards[(i + j) % numNodes]);
                }
            }
            shardMap = builder.build();

            // Compute reverse map.
            nodeSelectionPolicy = new DefaultNodeSelectionPolicy();
            for (int i = 0; i < numNodes; i++) {
                String thisShard = shards[i];
                List<String> nodeList = new ArrayList<>();
                for (int j = 0; j < numNodes; j++) {
                    if (shardMap.get(nodes[j]).contains(thisShard)) {
                        nodeList.add(nodes[j]);
                    }
                }
                nodeSelectionPolicy.update(thisShard, nodeList);
            }
            shardMapsFail = false;
        }

        public void setShardMapsFail(boolean shardMapsFail) {
            this.shardMapsFail = shardMapsFail;
        }

        @Override
        public ImmutableSetMultimap<String, String> createNode2ShardsMap(Collection<String> shards) throws ShardAccessException {
            if (shardMapsFail) {
                throw new ShardAccessException("Test error");
            }
            return nodeSelectionPolicy.createNode2ShardsMap(shards);
        }

        @Override
        public VersionedProtocol getProxy(String node, boolean establishIfNotExists) {
            return proxyProvider != null ? proxyProvider.getProxy(node) : null;
        }

        @Override
        public void reportNodeCommunicationFailure(String node, Throwable t) {
            nodeSelectionPolicy.removeNode(node);
        }

        public ImmutableSet<String> allShards() {
            return allShards;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void reportNodeCommunicationSuccess(String node) {

        }

    }

    public static class TestNodeInteractionFactory implements INodeInteractionFactory<Integer> {

        public List<Entry> calls = new ArrayList<>();
        public int maxSleep;
        public long additionalSleepTime = 0; // TODO combine sleeps

        public TestNodeInteractionFactory(int maxSleep) {
            this.maxSleep = maxSleep;
        }

        @Override
        public Runnable createInteraction(Method method, Object[] args, int shardArrayParamIndex, String node, ImmutableSetMultimap<String, String> nodeShardMap, int tryCount, int maxTryCount, INodeProxyManager shardManager, INodeExecutor nodeExecutor, IResultReceiverWrapper<Integer> results) {
            calls.add(new Entry(node, method, args));
            final long additionalSleepTime2 = additionalSleepTime;
            final TestServer server = new TestServer(maxSleep);
            final Set<String> shards = nodeShardMap.get(node);
            return () -> {
                if (additionalSleepTime2 > 0) {
                    sleep(additionalSleepTime2);
                }
                int n = (Integer) args[0];
                int r = server.doSomething(n);
                // System.out.printf("Test interaction, node=%s, f(%d)=%d, shards=%s\n",
                // node, n, r, shards);
                results.addNodeResult(r, shards);
            };
        }

        public List<Entry> getCalls() {
            return calls;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            String sep = "";
            for (Entry entry : calls) {
                sb.append(sep);
                sb.append(entry.toString());
                sep = ", ";
            }
            return sb.toString();
        }

        public class Entry {
            public String node;
            public Method method;
            public Object[] args;

            public Entry(String node, Method method, Object[] args) {
                this.node = node;
                this.method = method;
                this.args = args;
            }

            @Override
            public String toString() {
                return node + ":" + method.getName() + ":" + Arrays.asList(args).toString();
            }
        }

    }

    private static class TestServer {
        private static Random rand = new Random("testserver".hashCode());
        private int maxSleep;

        public TestServer(int maxSleep) {
            this.maxSleep = maxSleep;
        }

        public int doSomething(int n) {
            long msec = rand.nextInt(maxSleep);
            sleep(msec);
            return n * 2;
        }
    }

}
