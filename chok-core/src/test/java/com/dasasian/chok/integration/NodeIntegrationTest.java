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
import com.dasasian.chok.operation.node.ShardUndeployOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.TestUtil;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestClient;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestServer;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class NodeIntegrationTest extends AbstractTest {

    @Rule
    public ChokMiniCluster miniCluster = new ChokMiniCluster(SimpleTestServer.class, 2, 20000,TestNodeConfigurationFactory.class);

    @Test
    public void testContentServer() throws Exception {
        TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 2);
        miniCluster.deployIndex(testIndex, miniCluster.getRunningNodeCount());

        final InteractionProtocol protocol = miniCluster.getProtocol();
        assertEquals(1, protocol.getIndices().size());

        SimpleTestClient client = new SimpleTestClient(miniCluster.getZkConfiguration());
        assertEquals("query", client.testRequest("query", null));
        client.close();
    }

    @Test
    public void testDeployShardAfterRestart() throws Exception {
        TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 2);
        miniCluster.deployIndex(testIndex, miniCluster.getRunningNodeCount());

        final InteractionProtocol protocol = miniCluster.getProtocol();
        assertEquals(1, protocol.getIndices().size());

        Collection<String> deployedShards = protocol.getNodeShards(miniCluster.getNode(0).getName());
        assertFalse(deployedShards.isEmpty());

        // restart node
        Node node = miniCluster.restartNode(0);
        assertEquals(deployedShards, protocol.getNodeShards(node.getName()));
    }

    @Test
    public void testUndeployShard() throws Exception {
        TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 4);
        miniCluster.deployIndex(testIndex, miniCluster.getRunningNodeCount());

        final InteractionProtocol protocol = miniCluster.getProtocol();
        assertEquals(1, protocol.getIndices().size());
        Node node = miniCluster.getNode(0);
        TestUtil.waitUntilNodeServesShards(protocol, node.getName(), testIndex.getShardCount());

        // we should have 4 folders in our working folder now.
        File shardsFolder = node.getContext().getShardManager().getShardsFolder();
        assertEquals(testIndex.getShardCount(), shardsFolder.list().length);

        ShardUndeployOperation undeployOperation = new ShardUndeployOperation(Arrays.asList(protocol.getNodeShards(node.getName()).iterator().next()));
        protocol.addNodeOperation(node.getName(), undeployOperation);
        int expectedShardCount = testIndex.getShardCount() - 1;
        TestUtil.waitUntilNodeServesShards(protocol, node.getName(), expectedShardCount);
        // Thread.sleep(2000);
        assertEquals(expectedShardCount, shardsFolder.list().length);
    }

}
