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

import com.dasasian.chok.node.Node;
import com.dasasian.chok.operation.master.CheckIndicesOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.TestUtil;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexAutoRepairTest extends AbstractTest {

    @Rule
    public ChokMiniCluster miniCluster = new ChokMiniCluster(SimpleTestServer.class, 2, 20000, TestNodeConfigurationFactory.class);

    @Test
    public void testEnableDisableTest() {
        final InteractionProtocol protocol = miniCluster.getProtocol();
        assertTrue(protocol.isIndexAutoRepairEnabled());
        protocol.disableIndexAutoRepair();
        assertFalse(protocol.isIndexAutoRepairEnabled());
        protocol.enableIndexAutoRepair();
        assertTrue(protocol.isIndexAutoRepairEnabled());
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

        protocol.disableIndexAutoRepair();

        miniCluster.shutdownNode(0);
        assertTrue(optimumShardDeployCount > miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));

        Thread.sleep(2000);
        assertTrue(optimumShardDeployCount > miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));

        protocol.enableIndexAutoRepair();
        protocol.addMasterOperation(new CheckIndicesOperation());

        Thread.sleep(2000);
        Assert.assertEquals(optimumShardDeployCount, miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));
    }

    @Test
    public void testReplicateUnderreplicatedIndexesAfterNodeAdding() throws Exception {
        int replicationCount = miniCluster.getRunningNodeCount() + 1;

        TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 3);

        miniCluster.deployIndex(testIndex, replicationCount);

        final InteractionProtocol protocol = miniCluster.getProtocol();
        Assert.assertEquals(1, protocol.getIndices().size());

        int optimumShardDeployCount = testIndex.getShardCount() * replicationCount;
        Assert.assertTrue(optimumShardDeployCount > miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));

        protocol.disableIndexAutoRepair();

        Node node = miniCluster.startAdditionalNode();
        Thread.sleep(2000);
        assertTrue(optimumShardDeployCount > miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));

        protocol.enableIndexAutoRepair();
        protocol.addMasterOperation(new CheckIndicesOperation());

        TestUtil.waitUntilNodeServesShards(protocol, node.getName(), testIndex.getShardCount());
        Assert.assertEquals(optimumShardDeployCount, miniCluster.countShardDeployments(protocol, testIndex.getIndexName()));
    }
}
