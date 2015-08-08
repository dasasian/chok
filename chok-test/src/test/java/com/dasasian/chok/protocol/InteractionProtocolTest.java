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
package com.dasasian.chok.protocol;

import com.dasasian.chok.master.Master;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.node.monitor.MetricsRecord;
import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.master.AbstractIndexOperation;
import com.dasasian.chok.operation.master.MasterOperation;
import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.dasasian.chok.protocol.metadata.NodeMetaData;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.Mocks;
import com.dasasian.chok.testutil.ZkTestSystem;
import com.dasasian.chok.testutil.mockito.WaitingAnswer;
import com.dasasian.chok.util.ZkChokUtil;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.Gateway;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.util.ZkPathUtil;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.proto.WatcherEvent;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class InteractionProtocolTest extends AbstractTest {

    @Rule
    public ZkTestSystem zk = new ZkTestSystem();

    private InteractionProtocol protocol;

    @Before
    public void setProtocol() {
        protocol = zk.createInteractionProtocol();
    }

    @Test(timeout = 7000)
    public void testLifecycle() throws Exception {
        int GATEWAY_PORT = 2190;
        Gateway gateway = new Gateway(GATEWAY_PORT, zk.getServerPort());
        gateway.start();
        ZkClient zkClient = ZkChokUtil.startZkClient("localhost:" + GATEWAY_PORT);

        InteractionProtocol protocol = new InteractionProtocol(zkClient, zk.getZkConfiguration());
        final AtomicInteger connectCount = new AtomicInteger();
        final AtomicInteger disconnectCount = new AtomicInteger();
        final Object mutex = new Object();

        protocol.registerComponent(new ConnectedComponent() {
            @Override
            public void disconnect() {
                disconnectCount.incrementAndGet();
                synchronized (mutex) {
                    mutex.notifyAll();
                }
            }

            @Override
            public void reconnect() {
                connectCount.incrementAndGet();
                synchronized (mutex) {
                    mutex.notifyAll();
                }
            }
        });
        synchronized (mutex) {
            gateway.stop();
            mutex.wait();
            gateway.start();
            mutex.wait();
            gateway.stop();
            mutex.wait();
            gateway.start();
            mutex.wait();
        }
        zkClient.close();
        assertEquals(2, connectCount.get());
        assertEquals(2, connectCount.get());
        gateway.stop();
    }

    @Test(timeout = 70000)
    public void testNodeQueue() throws Exception {
        Node node = Mockito.mock(Node.class);
        String nodeName = "node1";
        Mockito.when(node.getName()).thenReturn(nodeName);

        NodeQueue nodeQueue = protocol.publishNode(node, new NodeMetaData());

        NodeOperation nodeOperation1 = Mockito.mock(NodeOperation.class, Mockito.withSettings().serializable().name("a"));
        NodeOperation nodeOperation2 = Mockito.mock(NodeOperation.class, Mockito.withSettings().serializable().name("b"));
        OperationId operation1Id = protocol.addNodeOperation(nodeName, nodeOperation1);
        OperationId operation2Id = protocol.addNodeOperation(nodeName, nodeOperation2);

        assertTrue(protocol.isNodeOperationQueued(operation1Id));
        assertTrue(protocol.isNodeOperationQueued(operation2Id));
        assertEquals(nodeOperation1.toString(), nodeQueue.remove().toString());
        assertEquals(nodeOperation2.toString(), nodeQueue.remove().toString());
        assertTrue(nodeQueue.isEmpty());
        assertFalse(protocol.isNodeOperationQueued(operation1Id));
        assertFalse(protocol.isNodeOperationQueued(operation2Id));
    }

    @Test(timeout = 7000)
    public void testPublishMaster() throws Exception {
        Master master1 = Mockito.mock(Master.class);
        Master master2 = Mockito.mock(Master.class);
        Mockito.when(master1.getMasterName()).thenReturn("master1");
        Mockito.when(master2.getMasterName()).thenReturn("master2");
        MasterOperation operation = Mockito.mock(MasterOperation.class);
        protocol.addMasterOperation(operation);

        MasterQueue queue = protocol.publishMaster(master1);
        assertNotNull(queue);
        assertNotNull(protocol.getMasterMD());
        assertEquals(1, queue.size());
        assertNotNull(queue.peek());

        // same again
        queue = protocol.publishMaster(master1);
        assertNotNull(queue);
        assertNotNull(protocol.getMasterMD());

        // second master
        queue = protocol.publishMaster(master2);
        assertNull(queue);
        assertNotNull(protocol.getMasterMD());
    }

    @Test(timeout = 7000)
    public void testPublishNode() throws Exception {
        Node node = Mocks.mockNode();
        assertNull(protocol.getNodeMD(node.getName()));
        NodeQueue queue = protocol.publishNode(node, new NodeMetaData(node.getName()));
        assertNotNull(queue);
        assertNotNull(protocol.getNodeMD(node.getName()));

        // test operation
        NodeOperation operation = Mockito.mock(NodeOperation.class);
        OperationId operationId = protocol.addNodeOperation(node.getName(), operation);
        assertEquals(1, queue.size());
        assertNotNull(queue.peek());
        assertEquals(node.getName(), operationId.getNodeName());
    }

    @Test(timeout = 7000)
    public void testExplainStructure() throws Exception {
        protocol.explainStructure();
        System.out.println("----------------");
        protocol.showStructure(false);
        System.out.println("----------------");
        protocol.showStructure(true);
    }

    @Test(timeout = 7000)
    public void testUnregisterListenersOnUnregisterComponent() throws Exception {
        ConnectedComponent component = Mockito.mock(ConnectedComponent.class);
        protocol.registerComponent(component);

        IAddRemoveListener childListener = Mockito.mock(IAddRemoveListener.class);
        IZkDataListener dataListener = Mockito.mock(IZkDataListener.class);
        protocol.registerChildListener(component, PathDef.NODES_LIVE, childListener);
        protocol.registerDataListener(component, PathDef.NODES_LIVE, "node1", dataListener);

        zk.getZkClient().createPersistent(zk.getZkConfiguration().getPath(PathDef.NODES_LIVE, "node1"));
        Thread.sleep(500);
        Mockito.verify(childListener).added("node1");
        Mockito.verify(dataListener).handleDataChange(Matchers.anyString(), Matchers.any());
        Mockito.verifyNoMoreInteractions(childListener, dataListener);

        protocol.unregisterComponent(component);
        zk.getZkClient().delete(zk.getZkConfiguration().getPath(PathDef.NODES_LIVE, "node1"));
        Thread.sleep(500);
        Mockito.verifyNoMoreInteractions(childListener, dataListener);
        // ephemerals should be removed
        // listeners nshould be removed
    }

    @Test(timeout = 7000)
    public void testDeleteEphemeralsOnUnregisterComponent() throws Exception {
        Master master = Mockito.mock(Master.class);
        protocol.publishMaster(master);
        assertNotNull(protocol.getMasterMD());

        protocol.unregisterComponent(master);
        assertNull(protocol.getMasterMD());
    }

    @Test(timeout = 7000)
    public void testChildListener() throws Exception {
        ConnectedComponent component = Mockito.mock(ConnectedComponent.class);
        IAddRemoveListener listener = Mockito.mock(IAddRemoveListener.class);
        PathDef pathDef = PathDef.NODES_LIVE;

        zk.getZkClient().createPersistent(zk.getZkConfiguration().getPath(pathDef, "node1"));
        List<String> existingChilds = protocol.registerChildListener(component, pathDef, listener);
        assertEquals(1, existingChilds.size());
        assertTrue(existingChilds.contains("node1"));

        zk.getZkClient().createPersistent(zk.getZkConfiguration().getPath(pathDef, "node2"));
        zk.getZkClient().delete(zk.getZkConfiguration().getPath(pathDef, "node1"));

        Thread.sleep(500);
        InOrder inOrder = Mockito.inOrder(listener);
        inOrder.verify(listener).added("node2");
        inOrder.verify(listener).removed("node1");
        Mockito.verifyNoMoreInteractions(listener);
    }

    @Test(timeout = 70000)
    public void testDataListener() throws Exception {
        ConnectedComponent component = Mockito.mock(ConnectedComponent.class);
        IZkDataListener listener = Mockito.mock(IZkDataListener.class);
        PathDef pathDef = PathDef.INDICES_METADATA;
        String zkPath = zk.getZkConfiguration().getPath(pathDef, "index1");

        Long serializable = (long) 1;
        zk.getZkClient().createPersistent(zkPath, serializable);
        protocol.registerDataListener(component, pathDef, "index1", listener);

        serializable = (long) 2;
        zk.getZkClient().writeData(zkPath, serializable);
        Thread.sleep(500);
        Mockito.verify(listener).handleDataChange(zkPath, serializable);

        zk.getZkClient().delete(zkPath);
        Thread.sleep(500);
        Mockito.verify(listener).handleDataDeleted(zkPath);
        Mockito.verifyNoMoreInteractions(listener);
    }

    @Test(timeout = 70000)
    public void testIndexManagement() throws Exception {
        IndexMetaData indexMD = new IndexMetaData("index1", new URI("indexPath"), 2, false);
        indexMD.getShards().add(new Shard(AbstractIndexOperation.createShardName(indexMD.getName(), "path1"), new URI("path1")));
        Node node = Mocks.mockNode();

        assertNull(protocol.getIndexMD("index1"));
        assertEquals(0, protocol.getIndices().size());

        // publish index
        protocol.publishIndex(indexMD);
        protocol.publishShard(node, indexMD.getShards().iterator().next().getName());
        assertNotNull(protocol.getIndexMD("index1"));
        assertEquals(1, protocol.getIndices().size());
        assertEquals(indexMD.getReplicationLevel(), protocol.getIndexMD(indexMD.getName()).getReplicationLevel());

        // update index
        indexMD.setReplicationLevel(3);
        protocol.updateIndexMD(indexMD);
        assertEquals(indexMD.getReplicationLevel(), protocol.getIndexMD(indexMD.getName()).getReplicationLevel());

        protocol.showStructure(false);
        protocol.unpublishIndex(indexMD.getName());
        protocol.showStructure(false);
        assertNull(protocol.getIndexMD("index1"));
        assertEquals(0, protocol.getIndices().size());

        String string = ZkPathUtil.toString(protocol.zkClient);
        Set<Shard> shards = indexMD.getShards();
        for (Shard shard : shards) {
            assertFalse(string.contains(shard.getName()));
        }
    }

    @Test(timeout = 7000)
    public void testShardManagement() throws Exception {
        Node node1 = Mocks.mockNode();
        Node node2 = Mocks.mockNode();

        assertEquals(0, protocol.getShardNodes("shard1").size());

        // publish shard
        protocol.publishShard(node1, "shard1");
        assertEquals(1, protocol.getShardNodes("shard1").size());
        assertEquals(1, protocol.getNodeShards(node1.getName()).size());
        assertEquals(0, protocol.getNodeShards(node2.getName()).size());

        // publish shard on 2nd node
        protocol.publishShard(node2, "shard1");
        assertEquals(2, protocol.getShardNodes("shard1").size());
        assertEquals(1, protocol.getNodeShards(node1.getName()).size());
        assertEquals(1, protocol.getNodeShards(node2.getName()).size());

        // remove shard on first node
        protocol.unpublishShard(node1, "shard1");
        assertEquals(1, protocol.getShardNodes("shard1").size());
        assertEquals(0, protocol.getNodeShards(node1.getName()).size());
        assertEquals(1, protocol.getNodeShards(node2.getName()).size());

        // publish 2nd shard
        protocol.publishShard(node1, "shard2");
        assertEquals(1, protocol.getShardNodes("shard1").size());
        assertEquals(1, protocol.getShardNodes("shard2").size());
        assertEquals(1, protocol.getNodeShards(node1.getName()).size());
        assertEquals(1, protocol.getNodeShards(node2.getName()).size());

        // remove one shard completely
        protocol.unpublishShard(node1, "shard2");

        Map<String, List<String>> shard2NodesMap = protocol.getShard2NodesMap(Collections.singletonList("shard1"));
        assertEquals(1, shard2NodesMap.size());
        assertEquals(1, shard2NodesMap.get("shard1").size());
    }

    @Test(timeout = 7000)
    public void testMetrics() throws Exception {
        String nodeName1 = "node1";
        assertNull(protocol.getMetric(nodeName1));
        protocol.setMetric(nodeName1, new MetricsRecord(nodeName1));
        assertNotNull(protocol.getMetric(nodeName1));

        String nodeName2 = "node2";
        protocol.setMetric(nodeName2, new MetricsRecord(nodeName1));
        assertNotSame(protocol.getMetric(nodeName1).getServerId(), protocol.getMetric(nodeName2).getServerId());
    }

    @Test
    /**see CHOK-125*/
    public void testConcurrentModification() throws Exception {
        ConnectedComponent component1 = Mockito.mock(ConnectedComponent.class);
        WaitingAnswer waitingAnswer = new WaitingAnswer();
        Mockito.doAnswer(waitingAnswer).when(component1).disconnect();
        protocol = zk.createInteractionProtocol();
        protocol.registerComponent(component1);
        WatchedEvent expiredEvent = new WatchedEvent(new WatcherEvent(EventType.None.getIntValue(), KeeperState.Expired.getIntValue(), null));
        protocol.getZkClient().process(new WatchedEvent(new WatcherEvent(EventType.None.getIntValue(), KeeperState.SyncConnected.getIntValue(), null)));
        protocol.getZkClient().process(expiredEvent);
        // verify(component1).disconnect();

        ConnectedComponent component2 = Mockito.mock(ConnectedComponent.class, "2ndComp");
        protocol.registerComponent(component2);
        protocol.unregisterComponent(component2);
        waitingAnswer.release();
        protocol.disconnect();
    }
}
