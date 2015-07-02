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
import com.dasasian.chok.node.Node;
import com.dasasian.chok.operation.master.IndexDeployOperation;
import com.dasasian.chok.operation.master.IndexUndeployOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexDeployError.ErrorType;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.TestUtil;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestResources;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestServer;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.UtilModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Set;

public class MasterIntegrationTest extends AbstractTest {

    protected static Injector injector = Guice.createInjector(new UtilModule());

    @Rule
    public ChokMiniCluster miniCluster = new ChokMiniCluster(SimpleTestServer.class, 2, 20000, TestNodeConfigurationFactory.class, injector.getInstance(ChokFileSystem.Factory.class));

    @Test(timeout = 20000)
    public void testDeployAndUndeployIndex() throws Exception {
        final InteractionProtocol protocol = miniCluster.getProtocol();

        TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 2);

        IndexDeployOperation deployOperation = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexUri(), miniCluster.getRunningNodeCount());
        protocol.addMasterOperation(deployOperation);

        TestUtil.waitUntilIndexDeployed(protocol, testIndex.getIndexName());
        Assert.assertEquals(1, protocol.getIndices().size());
        IndexMetaData indexMD = protocol.getIndexMD(testIndex.getIndexName());
        Assert.assertEquals(null, indexMD.getDeployError());
        Assert.assertEquals(testIndex.getShardCount(), indexMD.getShards().size());

        Set<Shard> shards = indexMD.getShards();
        for (Shard shard : shards) {
            Assert.assertEquals(miniCluster.getRunningNodeCount(), protocol.getShardNodes(shard.getName()).size());
            Assert.assertEquals(2, shard.getMetaDataMap().size());
        }

        // undeploy
        IndexUndeployOperation undeployOperation = new IndexUndeployOperation(testIndex.getIndexName());
        protocol.addMasterOperation(undeployOperation);
        TestUtil.waitUntilShardsUndeployed(protocol, indexMD);

        Assert.assertEquals(0, protocol.getIndices().size());
        Assert.assertEquals(null, protocol.getIndexMD(testIndex.getIndexName()));
        for (Shard shard : shards) {
            Assert.assertEquals(0, protocol.getShardNodes(shard.getName()).size());
        }
        Assert.assertEquals(0, protocol.getShard2NodeShards().size());
    }

    @Test(timeout = 20000)
    public void testDeployError() throws Exception {
        final InteractionProtocol protocol = miniCluster.getProtocol();

        final File indexFile = SimpleTestResources.INVALID_INDEX;
        String indexName = indexFile.getName();

        IDeployClient deployClient = new DeployClient(protocol);
        IIndexDeployFuture deployFuture = deployClient.addIndex(indexName, indexFile.toURI(), 1);
        deployFuture.joinDeployment();
        TestUtil.waitUntilIndexDeployed(protocol, indexName);
        Assert.assertEquals(1, protocol.getIndices().size());
        IndexMetaData indexMD = protocol.getIndexMD(indexName);
        Assert.assertNotNull(indexMD.getDeployError());
        Assert.assertEquals(ErrorType.SHARDS_NOT_DEPLOYABLE, indexMD.getDeployError().getErrorType());
    }

    @Test(timeout = 20000)
    public void testRebalanceIndexAfterNodeCrash() throws Exception {
        int replicationCount = miniCluster.getRunningNodeCount() - 1;

        TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 2);

        miniCluster.deployIndex(testIndex, 1);

        final InteractionProtocol protocol = miniCluster.getProtocol();
        Assert.assertEquals(1, protocol.getIndices().size());

        int optimumShardDeployCount = testIndex.getShardCount() * replicationCount;
        Assert.assertEquals(optimumShardDeployCount, miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));

        miniCluster.shutdownNode(0);
        Assert.assertTrue(optimumShardDeployCount > miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));

        Thread.sleep(2000);
        Assert.assertEquals(optimumShardDeployCount, miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));
    }

    @Test(timeout = 20000)
    public void testIndexPickupAfterMasterRestart() throws Exception {
        TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, miniCluster.getRunningNodeCount());

        miniCluster.deployIndex(testIndex, 1);

        final InteractionProtocol protocol = miniCluster.getProtocol();
        Assert.assertEquals(1, protocol.getIndices().size());

        miniCluster.restartMaster();
        Assert.assertEquals(1, protocol.getIndices().size());
        Assert.assertTrue(protocol.getReplicationReport(protocol.getIndexMD(testIndex.getIndexName())).isDeployed());
    }

    @Test
    public void testReplicateUnderreplicatedIndexesAfterNodeAdding() throws Exception {
        int replicationCount = miniCluster.getRunningNodeCount() + 1;

        TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 3);

        miniCluster.deployIndex(testIndex, replicationCount);

        final InteractionProtocol protocol = miniCluster.getProtocol();
        Assert.assertEquals(1, protocol.getIndices().size());

        int optimumShardDeployCount = testIndex.getShardCount() * replicationCount;
        printNodeShards();
        Assert.assertTrue(optimumShardDeployCount > miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));

        Node node = miniCluster.startAdditionalNode();
        TestUtil.waitUntilNodeServesShards(protocol, node.getName(), testIndex.getShardCount());
        printNodeShards();
        Assert.assertTrue(optimumShardDeployCount == miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));
    }

    public void printNodeShards() {
        for (Node node : miniCluster.getNodes()) {
            System.out.println(miniCluster.getProtocol().getNodeShards(node.getName()));
        }
    }

}
