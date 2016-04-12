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

import com.dasasian.chok.operation.master.CheckIndicesOperation;
import com.dasasian.chok.operation.master.MasterOperation;
import com.dasasian.chok.operation.master.RemoveObsoleteShardsOperation;
import com.dasasian.chok.protocol.IAddRemoveListener;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestMasterConfiguration;
import com.dasasian.chok.testutil.TestUtil;
import com.dasasian.chok.testutil.TestZkConfiguration;
import com.dasasian.chok.testutil.mockito.SleepingAnswer;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.UtilModule;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class MasterMockTest extends AbstractTest {

    private InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);

    protected static Injector injector = Guice.createInjector(new UtilModule());

    @Before
    public void setUp() throws IOException {
        Mockito.when(protocol.getZkConfiguration()).thenReturn(TestZkConfiguration.getTestConfiguration(temporaryFolder.newFolder()));
        ZkClient mock = Mockito.mock(ZkClient.class);
        Mockito.when(protocol.getZkClient()).thenReturn(mock);
    }

    @Test
    public void testBecomeMaster() throws Exception {
        final Master master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);

        MasterQueue masterQueue = mockBlockingOperationQueue();
        Mockito.when(protocol.publishMaster(master)).thenReturn(masterQueue);
        Mockito.when(protocol.getLiveNodes()).thenReturn(Collections.singletonList("node1"));

        master.start();
        assertTrue(master.isMaster());
        master.shutdown();
        assertFalse(master.isMaster());
    }

    @Test
    public void testBecomeSecMaster() throws Exception {
        final Master master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);

        Mockito.when(protocol.publishMaster(master)).thenReturn(null);
        Mockito.when(protocol.getLiveNodes()).thenReturn(Collections.singletonList("node1"));

        master.start();
        assertFalse(master.isMaster());
        master.shutdown();
    }

    @Test
    public void testDisconnectReconnect() throws Exception {
        final Master master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);

        MasterQueue masterQueue = mockBlockingOperationQueue();
        Mockito.when(protocol.publishMaster(master)).thenReturn(masterQueue);
        Mockito.when(protocol.getLiveNodes()).thenReturn(Collections.singletonList("node1"));

        master.start();
        assertTrue(master.isMaster());

        master.disconnect();
        assertFalse(master.isMaster());

        master.reconnect();
        assertTrue(master.isMaster());

        master.shutdown();
    }

    @Test
    public void testGracefulStartupShutdown() throws Exception {
        boolean shutdownClient = false;
        checkStartStop(shutdownClient, null);
    }

    @Test
    public void testGracefulStartupShutdownWithShutdownClient() throws Exception {
        boolean shutdownClient = true;
        checkStartStop(shutdownClient, null);
    }

    @Test
    public void testGracefulStartupShutdownWithZkServer() throws Exception {
        ZkServer zkServer = Mockito.mock(ZkServer.class);
        checkStartStop(false, zkServer);
    }

    @Test
    public void testRemoveOldNodeShards() throws Exception {
        final Master master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);

        String nodeName = "node1";
        MasterQueue masterQueue = mockBlockingOperationQueue();
        Mockito.when(protocol.publishMaster(master)).thenReturn(masterQueue);
        Mockito.when(protocol.getLiveNodes()).thenReturn(Collections.singletonList(nodeName));
        Mockito.when(protocol.registerChildListener(Matchers.eq(master), Matchers.eq(PathDef.NODES_LIVE), Matchers.any(IAddRemoveListener.class))).thenReturn(Collections.singletonList(nodeName));

        List<String> shards = Arrays.asList("shard1", "shard2");
        Mockito.when(protocol.getNodeShards(nodeName)).thenReturn(shards);
        master.start();

        assertTrue(master.isMaster());
        TestUtil.waitUntilLeaveSafeMode(master);
        ArgumentCaptor<MasterOperation> argument = ArgumentCaptor.forClass(MasterOperation.class);
        Mockito.verify(protocol, Mockito.times(2)).addMasterOperation(argument.capture());
        assertTrue(argument.getAllValues().get(0) instanceof CheckIndicesOperation);
        assertTrue(argument.getAllValues().get(1) instanceof RemoveObsoleteShardsOperation);
        assertEquals(nodeName, ((RemoveObsoleteShardsOperation) argument.getAllValues().get(1)).getNodeName());
        master.shutdown();
    }

    private void checkStartStop(boolean shutdownClient, ZkServer zkServer) throws Exception {
        final Master master;
        if (zkServer != null) {
            master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), zkServer, false);
        } else {
            master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), shutdownClient);
        }

        MasterQueue masterQueue = mockBlockingOperationQueue();
        Mockito.when(protocol.publishMaster(master)).thenReturn(masterQueue);
        Mockito.when(protocol.getLiveNodes()).thenReturn(Collections.singletonList("node1"));

        master.start();
        TestUtil.waitUntilLeaveSafeMode(master);

        // stop agin
        master.shutdown();
        Mockito.verify(protocol).unregisterComponent(master);
        if (shutdownClient) {
            Mockito.verify(protocol).disconnect();
        }
        if (zkServer != null) {
            Mockito.verify(zkServer).shutdown();
        }
    }

    private MasterQueue mockBlockingOperationQueue() throws InterruptedException {
        MasterQueue queue = Mockito.mock(MasterQueue.class);
        Mockito.when(queue.peek()).thenAnswer(new SleepingAnswer());
        return queue;
    }

}
