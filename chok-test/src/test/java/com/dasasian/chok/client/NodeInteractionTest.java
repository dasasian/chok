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
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link NodeInteraction}.
 */
public class NodeInteractionTest extends AbstractTest {

    private TestProxyProvider proxyProvider;
    private WorkQueueTest.TestShardManager shardManager;
    private TestNodeExecutor nodeExecutor;
    private Map<String, List<String>> node2ShardsMap;

    @Before
    public void setUp() throws Exception {
        proxyProvider = new TestProxyProvider();
        shardManager = new WorkQueueTest.TestShardManager(proxyProvider, 8, 3);
        nodeExecutor = new TestNodeExecutor();
        node2ShardsMap = shardManager.createNode2ShardsMap(shardManager.allShards());
    }

    @Test
    public void testNormalCall() throws Exception {
        Method method = ITestServer.class.getMethod("testMethod", String.class, String[].class);
        Object[] args = new Object[]{"foo", null};
        ClientResult<String> r = new ClientResult<>(null, shardManager.allShards());
        Runnable ni = new NodeInteraction<>(method, args, 1, "n1", node2ShardsMap, 1, 3, shardManager, nodeExecutor, r);
        assertEquals("NodeInteraction: call testMethod on n1", ni.toString());
        ni.run();
        assertEquals("ClientResult: 1 results, 0 errors, 3/8 shards", r.toString());
        assertEquals("n1:foo:[s1, s2, s3]", proxyProvider.toString());
        assertEquals("", nodeExecutor.toString());
    }

    @Test
    public void testNormalCallNoShardsParam() throws Exception {
        Method method = ITestServer.class.getMethod("testMethodNoShards", String.class);
        Object[] args = new Object[]{"foo"};
        ClientResult<String> r = new ClientResult<>(null, shardManager.allShards());
        Runnable ni = new NodeInteraction<>(method, args, -1, "n1", node2ShardsMap, 1, 3, shardManager, nodeExecutor, r);
        assertEquals("NodeInteraction: call testMethodNoShards on n1", ni.toString());
        ni.run();
        assertEquals("ClientResult: 1 results, 0 errors, 3/8 shards", r.toString());
        assertEquals("n1:foo:null", proxyProvider.toString());
        assertEquals("", nodeExecutor.toString());
    }

    @Test
    public void testRetries() throws Exception {
        Method method = ITestServer.class.getMethod("failingMethod", String.class, String[].class);
        int maxTryCount = 3;
        Object[] args = new Object[]{"foo", null};
    /*
     * First try to call node n1 with shards s1, s2, s3. TryCount = 1. Node
     * fails.
     */
        ClientResult<String> r = new ClientResult<>(null, shardManager.allShards());
        assertEquals(3, node2ShardsMap.get("n1").size());
        assertTrue(node2ShardsMap.get("n1").contains("s1"));
        assertTrue(node2ShardsMap.get("n1").contains("s2"));
        assertTrue(node2ShardsMap.get("n1").contains("s3"));
        Runnable ni = new NodeInteraction<>(method, args, 1, "n1", node2ShardsMap, 1, maxTryCount, shardManager, nodeExecutor, r);
        assertEquals("NodeInteraction: call failingMethod on n1", ni.toString());
        ni.run();
        assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", r.toString());
        assertEquals("n1:null:null", proxyProvider.toString());
        assertEquals("n3:2:{n3=[s3], n8=[s1, s2]}, n8:2:{n3=[s3], n8=[s1, s2]}", nodeExecutor.toString());
        List<NodeInteractionTest.TestNodeExecutor.Call> retriesA = nodeExecutor.calls;
    /*
     * Now simulate running the 2 retries. TryCount = 2. Node n3 with shard s3.
     * Node fails.
     */
        nodeExecutor = new TestNodeExecutor();
        NodeInteractionTest.TestNodeExecutor.Call call = retriesA.get(0);
        assertEquals("n3", call.node);
        assertEquals(1, call.nodeShardMap.get(call.node).size());
        assertTrue(call.nodeShardMap.get(call.node).contains("s3"));
        r = new ClientResult<>(null, shardManager.allShards());
        ni = new NodeInteraction<>(method, args, 1, call.node, call.nodeShardMap, 2, maxTryCount, shardManager, nodeExecutor, r);
        ni.run();
        assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", r.toString());
        assertEquals("n1:null:null, n3:null:null", proxyProvider.toString());
        assertEquals("n2:3:{n2=[s3]}", nodeExecutor.toString());
        NodeInteractionTest.TestNodeExecutor.Call retryB1 = nodeExecutor.calls.get(0);
    /*
     * Second retry. TryCount = 2. Node n8 with shards s1, s2. Node fails.
     */
        nodeExecutor = new TestNodeExecutor();
        call = retriesA.get(1);
        assertEquals("n8", call.node);
        assertEquals(2, call.nodeShardMap.get(call.node).size());
        assertTrue(call.nodeShardMap.get(call.node).contains("s1"));
        assertTrue(call.nodeShardMap.get(call.node).contains("s2"));
        r = new ClientResult<>(null, shardManager.allShards());
        ni = new NodeInteraction<>(method, args, 1, call.node, call.nodeShardMap, 2, maxTryCount, shardManager, nodeExecutor, r);
        ni.run();
        assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", r.toString());
        assertEquals("n1:null:null, n3:null:null, n8:null:null", proxyProvider.toString());
        assertEquals("n2:3:{n2=[s2], n7=[s1]}, n7:3:{n2=[s2], n7=[s1]}", nodeExecutor.toString());
        List<NodeInteractionTest.TestNodeExecutor.Call> retriesB2 = nodeExecutor.calls;
    /*
     * Third round of retries. TryCount = 3. No further retry attempts. Node n2
     * with shard s3. Node fails.
     */
        nodeExecutor = new TestNodeExecutor();
        assertEquals("n2", retryB1.node);
        assertEquals(1, retryB1.nodeShardMap.get(retryB1.node).size());
        assertTrue(retryB1.nodeShardMap.get(retryB1.node).contains("s3"));
        r = new ClientResult<>(null, shardManager.allShards());
        ni = new NodeInteraction<>(method, args, 1, retryB1.node, retryB1.nodeShardMap, 3, maxTryCount, shardManager, nodeExecutor, r);
        ni.run();
        assertEquals("ClientResult: 0 results, 1 errors, 1/8 shards", r.toString());
        assertEquals("n1:null:null, n2:null:null, n3:null:null, n8:null:null", proxyProvider.toString());
        assertEquals("", nodeExecutor.toString());
    /*
     * Node n2 with shard s2. TryCount = 3. Node fails.
     */
        nodeExecutor = new TestNodeExecutor();
        call = retriesB2.get(0);
        assertEquals("n2", call.node);
        assertEquals(1, call.nodeShardMap.get(call.node).size());
        assertTrue(call.nodeShardMap.get(call.node).contains("s2"));
        r = new ClientResult<>(null, shardManager.allShards());
        ni = new NodeInteraction<>(method, args, 1, call.node, call.nodeShardMap, 3, maxTryCount, shardManager, nodeExecutor, r);
        ni.run();
        assertEquals("ClientResult: 0 results, 1 errors, 1/8 shards", r.toString());
        assertEquals("n1:null:null, n2:null:null, n3:null:null, n8:null:null", proxyProvider.toString());
        assertEquals("", nodeExecutor.toString());
    /*
     * Node n7 with shard s1. TryCount = 3. Node fails.
     */
        nodeExecutor = new TestNodeExecutor();
        call = retriesB2.get(1);
        assertEquals("n7", call.node);
        assertEquals(1, call.nodeShardMap.get(call.node).size());
        assertTrue(call.nodeShardMap.get(call.node).contains("s1"));
        r = new ClientResult<>(null, shardManager.allShards());
        ni = new NodeInteraction<>(method, args, 1, call.node, call.nodeShardMap, 3, maxTryCount, shardManager, nodeExecutor, r);
        ni.run();
        assertEquals("ClientResult: 0 results, 1 errors, 1/8 shards", r.toString());
        assertEquals("n1:null:null, n2:null:null, n3:null:null, n7:null:null, n8:null:null", proxyProvider.toString());
        assertEquals("", nodeExecutor.toString());
    }

    @Test
    public void testMaxRetries() throws Exception {
        Method method = ITestServer.class.getMethod("failingMethod", String.class, String[].class);
        int maxTryCount = 2;
        Object[] args = new Object[]{"foo", null};
    /*
     * First try to call node n1 with shards s1, s2, s3. TryCount = 1. Node
     * fails.
     */
        ClientResult<String> r = new ClientResult<>(null, shardManager.allShards());
        assertEquals(3, node2ShardsMap.get("n1").size());
        assertTrue(node2ShardsMap.get("n1").contains("s1"));
        assertTrue(node2ShardsMap.get("n1").contains("s2"));
        assertTrue(node2ShardsMap.get("n1").contains("s3"));
        Runnable ni = new NodeInteraction<>(method, args, 1, "n1", node2ShardsMap, 1, maxTryCount, shardManager, nodeExecutor, r);
        assertEquals("NodeInteraction: call failingMethod on n1", ni.toString());
        ni.run();
        assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", r.toString());
        assertEquals("n1:null:null", proxyProvider.toString());
        assertEquals("n3:2:{n3=[s3], n8=[s1, s2]}, n8:2:{n3=[s3], n8=[s1, s2]}", nodeExecutor.toString());
        List<NodeInteractionTest.TestNodeExecutor.Call> retriesA = nodeExecutor.calls;
    /*
     * Now simulate running the 2 retries. TryCount = 2. Node n3 with shard s3.
     * Node fails.
     */
        nodeExecutor = new TestNodeExecutor();
        NodeInteractionTest.TestNodeExecutor.Call call = retriesA.get(0);
        assertEquals("n3", call.node);
        assertEquals(1, call.nodeShardMap.get(call.node).size());
        assertTrue(call.nodeShardMap.get(call.node).contains("s3"));
        r = new ClientResult<>(null, shardManager.allShards());
        ni = new NodeInteraction<>(method, args, 1, call.node, call.nodeShardMap, 2, maxTryCount, shardManager, nodeExecutor, r);
        ni.run();
        assertEquals("ClientResult: 0 results, 1 errors, 1/8 shards", r.toString());
        assertEquals("n1:null:null, n3:null:null", proxyProvider.toString());
        assertEquals("", nodeExecutor.toString());
    }

    @Test
    public void testRetriesUserClosedResult() throws Exception {
        Method method = TestServer.class.getMethod("failingMethod", String.class, String[].class);
        Object[] args = new Object[]{"foo", null};
    /*
     * Close the result object. Then try to call node n1 with shards s1, s2, s3.
     * TryCount = 1. Node fails. No retries should be attempted.
     */
        ClientResult<String> r = new ClientResult<>(null, shardManager.allShards());
        r.close();
        assertEquals(3, node2ShardsMap.get("n1").size());
        assertTrue(node2ShardsMap.get("n1").contains("s1"));
        assertTrue(node2ShardsMap.get("n1").contains("s2"));
        assertTrue(node2ShardsMap.get("n1").contains("s3"));
        Runnable ni = new NodeInteraction<>(method, args, 1, "n1", node2ShardsMap, 1, 3, shardManager, nodeExecutor, r);
        assertEquals("NodeInteraction: call failingMethod on n1", ni.toString());
        ni.run();
        assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards (closed)", r.toString());
        assertEquals("n1:null:null", proxyProvider.toString());
        assertEquals("", nodeExecutor.toString());
    }

    @Test
    public void testRetriesPolicyFailure() throws Exception {
        shardManager.setShardMapsFail(true);
        Method method = ITestServer.class.getMethod("failingMethod", String.class, String[].class);
        Object[] args = new Object[]{"foo", null};
    /*
     * Try to call node n1 with shards s1, s2, s3. TryCount = 1. Node fails.
     * When attempting to create retry node shard map, policy will throw an
     * exception. Give up on retries and log error.
     */
        ClientResult<String> r = new ClientResult<>(null, shardManager.allShards());
        assertEquals(3, node2ShardsMap.get("n1").size());
        assertTrue(node2ShardsMap.get("n1").contains("s1"));
        assertTrue(node2ShardsMap.get("n1").contains("s2"));
        assertTrue(node2ShardsMap.get("n1").contains("s3"));
        Runnable ni = new NodeInteraction<>(method, args, 1, "n1", node2ShardsMap, 1, 3, shardManager, nodeExecutor, r);
        assertEquals("NodeInteraction: call failingMethod on n1", ni.toString());
        ni.run();
        assertEquals("ClientResult: 0 results, 1 errors, 3/8 shards", r.toString());
        assertEquals("com.dasasian.chok.client.ShardAccessException: Shard 'Test error' is currently not reachable", r.getErrors().iterator().next().toString());
        assertEquals("n1:null:null", proxyProvider.toString());
        assertEquals("", nodeExecutor.toString());
    }

    @Test
    public void testNoProxy() throws Exception {
        proxyProvider.returnNullFor("n1");
        Method method = ITestServer.class.getMethod("testMethod", String.class, String[].class);
        Object[] args = new Object[]{"foo", null};
        ClientResult<String> r = new ClientResult<>(null, shardManager.allShards());
        Runnable ni = new NodeInteraction<>(method, args, 1, "n1", node2ShardsMap, 1, 3, shardManager, nodeExecutor, r);
        assertEquals("NodeInteraction: call testMethod on n1", ni.toString());
        ni.run();
        assertEquals(2, nodeExecutor.calls.size());
        // assertEquals("ClientResult: 0 results, 1 errors, 3/8 shards",
        // r.toString());
        assertEquals("", proxyProvider.toString());
        // assertEquals("[net.sf.chok.util.ChokException: No proxy for node: n1]",
        // r.getErrors().toString());
    }

    @Test
    public void testDefensiveArgCopy() throws Exception {
        Method method = ITestServer.class.getMethod("testMethod", String.class, String[].class);
        Object[] args = new Object[]{"OK", null};
        ClientResult<String> r = new ClientResult<>(null, shardManager.allShards());
        Runnable ni = new NodeInteraction<>(method, args, 1, "n1", node2ShardsMap, 1, 3, shardManager, nodeExecutor, r);
        assertEquals("NodeInteraction: call testMethod on n1", ni.toString());
        args[0] = "FAIL";
        ni.run();
        assertEquals("ClientResult: 1 results, 0 errors, 3/8 shards", r.toString());
        assertEquals("n1:OK:[s1, s2, s3]", proxyProvider.toString());
        assertEquals("", nodeExecutor.toString());
    }

    public interface ITestServer extends VersionedProtocol {
        String testMethod(String param, String[] shards);

        String testMethodNoShards(String param);

        String failingMethod(String param, String[] shards);
    }

    protected static class TestNodeExecutor implements INodeExecutor {

        protected List<Call> calls = new ArrayList<>();

        public void execute(String node, Map<String, List<String>> nodeShardMap, int tryCount, int maxTryCount) {
            Call call = new Call();
            call.node = node;
            call.nodeShardMap = nodeShardMap;
            call.tryCount = tryCount;
            calls.add(call);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            String sep = "";
            for (Call call : calls) {
                sb.append(sep);
                sb.append(call.toString());
                sep = ", ";
            }
            return sb.toString();
        }

        protected class Call {
            protected String node;
            protected Map<String, List<String>> nodeShardMap;
            protected int tryCount;

            @Override
            public String toString() {
                return node + ":" + tryCount + ":" + nodeShardMap;
            }
        }
    }

    private static class TestServer implements ITestServer, InvocationHandler {

        private String _node;
        private String _param;
        private String[] _shards;

        public TestServer(String node) {
            this._node = node;
        }

        public String testMethod(String param, String[] shards) {
            this._param = param;
            this._shards = shards;
            return "bar";
        }

        public String testMethodNoShards(String param) {
            this._param = param;
            this._shards = null;
            return "bar";
        }

        public String failingMethod(String param, String[] shards) {
            throw new RuntimeException("test exception");
        }

        @Override
        public String toString() {
            List<String> shards = null;
            if (_shards != null) {
                shards = Arrays.asList(_shards);
                Collections.sort(shards);
            }
            return _node + ":" + _param + ":" + (_shards != null ? shards.toString() : "null");
        }

        @Override
        public long getProtocolVersion(String arg0, long arg1) {
            return 0;
        }

        @Override
        public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
            return null;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            switch (name) {
                case "testMethod":
                    return testMethod((String) args[0], (String[]) args[1]);
                case "testMethodNoShards":
                    return testMethodNoShards((String) args[0]);
                case "failingMethod":
                    return failingMethod((String) args[0], (String[]) args[1]);
                case "toString":
                    return toString();
                default:
                    throw new RuntimeException("No method '" + name + "' in TestServer");
            }
        }
    }

    public static class TestProxyProvider implements WorkQueueTest.ProxyProvider {

        private Map<String, VersionedProtocol> proxyCache = new HashMap<>();
        private Map<String, TestServer> serverCache = new HashMap<>();
        private Set<String> returnNullNodes = new HashSet<>();

        public VersionedProtocol getProxy(String node) {
            if (returnNullNodes.contains(node)) {
                return null;
            }
            VersionedProtocol vp = proxyCache.get(node);
            if (vp != null) {
                return vp;
            }
            TestServer ts = new TestServer(node);
            serverCache.put(node, ts);
            vp = (VersionedProtocol) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{ITestServer.class}, ts);
            proxyCache.put(node, vp);
            return vp;
        }

        public TestServer getServer(String node) {
            return serverCache.get(node);
        }

        protected void returnNullFor(String node) {
            proxyCache.remove(node);
            returnNullNodes.add(node);
        }

        @Override
        public String toString() {
            List<String> nodes = new ArrayList<>(serverCache.keySet());
            Collections.sort(nodes);
            StringBuilder sb = new StringBuilder();
            String sep = "";
            for (String node : nodes) {
                sb.append(sep);
                sb.append(serverCache.get(node));
                sep = ", ";
            }
            return sb.toString();
        }

    }

}
