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
package com.dasasian.chok.master;

import com.dasasian.chok.node.Node;
import com.dasasian.chok.operation.master.IndexDeployOperation;
import com.dasasian.chok.operation.master.MasterOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.protocol.metadata.NodeMetaData;
import com.dasasian.chok.testutil.*;
import com.dasasian.chok.testutil.mockito.SerializableCountDownLatchAnswer;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestServer;
import com.dasasian.chok.util.*;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.I0Itec.zkclient.Gateway;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MasterZkTest extends AbstractZkTest {

    protected static Injector injector = Guice.createInjector(new UtilModule());

    public final TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 4);
    private final NodeConfigurationFactory nodeConfigurationFactory = new TestNodeConfigurationFactory(temporaryFolder);

    @Test
    public void testShutdown_shouldCleanupZkClientSubscriptions() throws ChokException {
        int numberOfListeners = zk.getZkClient().numberOfListeners();
        Master master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);
        master.start();
        master.shutdown();
        assertEquals(numberOfListeners, zk.getZkClient().numberOfListeners());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 10000)
    public void testMasterOperationPickup() throws Exception {
        Master master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);
        Node node = Mocks.mockNode();// leave safe mode
        protocol.publishNode(node, new NodeMetaData("node1"));
        master.start();


        final SerializableCountDownLatchAnswer answer = new SerializableCountDownLatchAnswer(2);

        MasterOperation operation1 = new TestMasterOperation(answer);
        MasterOperation operation2 = new TestMasterOperation(answer);

//        MasterOperation operation1 = mock(MasterOperation.class, withSettings().serializable());
//        MasterOperation operation2 = mock(MasterOperation.class, withSettings().serializable());
//        when(operation1.getExecutionInstruction((List<MasterOperation>) notNull())).thenReturn(ExecutionInstruction.EXECUTE);
//        when(operation2.getExecutionInstruction((List<MasterOperation>) notNull())).thenReturn(ExecutionInstruction.EXECUTE);
//        when(operation1.execute((MasterContext) notNull(), (List<MasterOperation>) notNull())).thenAnswer(answer);
//        when(operation2.execute((MasterContext) notNull(), (List<MasterOperation>) notNull())).thenAnswer(answer);

        protocol.addMasterOperation(operation1);
        protocol.addMasterOperation(operation2);
        answer.getCountDownLatch().await();

        master.shutdown();
    }


    @Test(timeout = 500000)
    public void testMasterChange_OnSessionReconnect() throws Exception {
        Master master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);
        Node node = Mocks.mockNode();// leave safe mode
        protocol.publishNode(node, new NodeMetaData("node1"));
        master.start();
        TestUtil.waitUntilLeaveSafeMode(master);

        Master secMaster = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);
        secMaster.start();

        master.disconnect();
        master.reconnect();
        master.shutdown();
        secMaster.shutdown();
    }

    @Test(timeout = 50000)
    public void testMasterChangeWhileDeploingIndex() throws Exception {
        Master master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);
        Node node = Mocks.mockNode();// leave safe mode
        NodeQueue nodeQueue = protocol.publishNode(node, new NodeMetaData("node1"));
        master.start();
        TestUtil.waitUntilLeaveSafeMode(master);

        // phase I - until watchdog is running and its node turn
        IndexDeployOperation deployOperation = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexUri(), 1);
        protocol.addMasterOperation(deployOperation);
        while (!master.getContext().getMasterQueue().isEmpty()) {
            // wait until deploy is in watch phase
            Thread.sleep(100);
        }

        // phase II - master change while node is deploying
        master.shutdown();
        Master secMaster = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);
        secMaster.start();

        // phase III - finish node operations/ mater operation should be finished
        while (!nodeQueue.isEmpty()) {
            nodeQueue.remove();
        }
        TestUtil.waitUntilIndexDeployed(protocol, deployOperation.getIndexName());
        assertNotNull(protocol.getIndexMD(deployOperation.getIndexName()));

        secMaster.shutdown();
    }

    @Test(timeout = 50000)
    public void testReconnectNode() throws Exception {
        final int GATEWAY_PORT = 2190;
        final Master master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);
        String zkRootPath = zk.getZkConfiguration().getRootPath();
        int serverPort = zk.getServerPort();

        // startup node over gateway
        final ZkConfiguration gatewayConf = TestZkConfiguration.getTestConfiguration(temporaryFolder.newFolder(), GATEWAY_PORT, zkRootPath);
        Gateway gateway = new Gateway(GATEWAY_PORT, serverPort);
        gateway.start();
        final ZkClient zkGatewayClient = ZkChokUtil.startZkClient(gatewayConf, 30000);
        InteractionProtocol gatewayProtocol = new InteractionProtocol(zkGatewayClient, gatewayConf);
        final Node node = new Node(gatewayProtocol, nodeConfigurationFactory.getConfiguration(), new SimpleTestServer(), injector.getInstance(ChokFileSystem.Factory.class));
        node.start();

        // check node-master link
        master.start();
        TestUtil.waitUntilLeaveSafeMode(master);
        TestUtil.waitUntilNumberOfLiveNode(protocol, 1);
        assertEquals(1, protocol.getLiveNodes().size());

        // now break the node connection
        gateway.stop();
        TestUtil.waitUntilNumberOfLiveNode(protocol, 0);

        // now fix the node connection
        gateway.start();
        TestUtil.waitUntilNumberOfLiveNode(protocol, 1);

        // cleanup
        node.shutdown();
        master.shutdown();
        zkGatewayClient.close();
        gateway.stop();
    }

}
