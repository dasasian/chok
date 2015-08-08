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
package com.dasasian.chok.integration;

import com.dasasian.chok.client.DeployClient;
import com.dasasian.chok.client.IDeployClient;
import com.dasasian.chok.client.IIndexDeployFuture;
import com.dasasian.chok.client.IndexState;
import com.dasasian.chok.master.Master;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexDeployError;
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.dasasian.chok.testutil.*;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestClient;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestServer;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.UtilModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.proto.WatcherEvent;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class FailoverTest extends AbstractTest {

    protected Injector injector = Guice.createInjector(new UtilModule());

    @Rule
    public ChokMiniCluster miniCluster = new ChokMiniCluster(SimpleTestServer.class, 3, 20000, TestNodeConfigurationFactory.class, injector.getInstance(ChokFileSystem.Factory.class));

    public TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 3);


    @Test(timeout = 20000)
    public void testMasterFail() throws Exception {
        // start secondary master..
        Master secondaryMaster = miniCluster.startSecondaryMaster();
        assertTrue(miniCluster.getMaster().isMaster());
        assertFalse(secondaryMaster.isMaster());

        // kill master
        miniCluster.getMaster().shutdown();

        // just make sure we can read the file
        TestUtil.waitUntilBecomeMaster(secondaryMaster);
    }

    @Test(timeout = 50000)
    public void testNodeFailure() throws Exception {
        miniCluster.deployIndex(testIndex, 3);

        final SimpleTestClient client = new SimpleTestClient(miniCluster.getZkConfiguration());
        assertEquals("1", client.testRequest("1", null));

        // kill 1st of 3 nodes
        miniCluster.shutdownNode(0);
        assertEquals("1", client.testRequest("1", null));

        // kill 2nd of 3 nodes
        miniCluster.shutdownNode(0);
        assertEquals("1", client.testRequest("1", null));

        // add a 4th node
        Node node4 = miniCluster.startAdditionalNode();
        TestUtil.waitUntilNodeServesShards(miniCluster.getProtocol(), node4.getName(), testIndex.getShardCount());
        assertEquals("1", client.testRequest("1", null));

        // kill 3rd node
        Thread.sleep(5000);
        miniCluster.shutdownNode(0);
        assertEquals("1", client.testRequest("1", null));

        client.close();
    }

    @Test(timeout = 100000)
    public void testZkMasterReconnectDuringDeployment() throws Exception {
        miniCluster.deployIndex(testIndex, miniCluster.getRunningNodeCount());

        miniCluster.getMaster().shutdown();

        ZkClient zkClient = new ZkClient(miniCluster.getZkConfiguration().getServers());
        InteractionProtocol protocol = new InteractionProtocol(zkClient, miniCluster.getZkConfiguration());
        Master master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, injector.getInstance(ChokFileSystem.Factory.class), false);
        master.start();
        TestUtil.waitUntilBecomeMaster(master);

        final IDeployClient deployClient = new DeployClient(miniCluster.getProtocol());
        WatchedEvent event = new WatchedEvent(new WatcherEvent(EventType.None.getIntValue(), KeeperState.Expired.getIntValue(), null));
        for (int i = 0; i < 25; i++) {
            final String indexName = "index" + i;
            IIndexDeployFuture deployFuture = deployClient.addIndex(indexName, testIndex.getIndexUri(), 1, false);
            zkClient.getEventLock().lock();
            zkClient.process(event);
            zkClient.getEventLock().unlock();
            IndexState indexState = deployFuture.joinDeployment();
            assertEquals("" + deployClient.getIndexMetaData(indexName).getDeployError(), IndexState.DEPLOYED, indexState);

            if (indexState == IndexState.ERROR) {
                IndexDeployError deployError = protocol.getIndexMD(indexName).getDeployError();
                Set<Shard> shards = protocol.getIndexMD(indexName).getShards();
                for (Shard shard : shards) {
                    List<Exception> shardErrors = deployError.getShardErrors(shard.getName());
                    for (Exception errorDetail : shardErrors) {
                        errorDetail.printStackTrace();
                    }
                }
                System.out.println(deployError.getErrorTrace());
            }
        }

        master.shutdown();

        zkClient.close();
    }

}
