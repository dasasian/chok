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
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.TestUtil;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestServer;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.UtilModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DeployPolicyIntegrationTest extends AbstractTest {

    protected static Injector injector = Guice.createInjector(new UtilModule());

    @Rule
    public ChokMiniCluster miniCluster = new ChokMiniCluster(SimpleTestServer.class, 2, 20000, TestNodeConfigurationFactory.class, injector.getInstance(ChokFileSystem.Factory.class));

    public TestIndex testIndexWith1Shard = TestIndex.createTestIndex(temporaryFolder, 1);
    public TestIndex testIndexWith2Shards = TestIndex.createTestIndex(temporaryFolder, 2);

    @Test
    public void testEqualDistribution3WhenMoreNodesThenShards() throws Exception {
        int replicationCount = 1;
        miniCluster.deployIndex(testIndexWith1Shard, replicationCount);

        final InteractionProtocol protocol = miniCluster.getProtocol();
        // todo this could be better
        assertTrue("node count=" + miniCluster.getRunningNodeCount() + " not greater than number of indices " + protocol.getIndices().size(),
                miniCluster.getRunningNodeCount() > protocol.getIndices().size());

//        protocol.showStructure(false);
        List<Node> nodes = miniCluster.getNodes();
        for (Node node : nodes) {
            assertTrue("Node has " + node.getContext().getContentServer().getShards().size() + " shard",
                    1 >= node.getContext().getContentServer().getShards().size());
        }
    }

    @Test
    public void testCopyToAllNodes() throws Exception {
        int replicationCount = IndexMetaData.REPLICATE_TO_ALL_NODES;
        miniCluster.deployIndex(testIndexWith2Shards, replicationCount);

        final InteractionProtocol protocol = miniCluster.getProtocol();
//        protocol.showStructure(false);

        assertThat(miniCluster.getStartedNodeCount(), is(equalTo(2)));

        List<Node> nodes = miniCluster.getNodes();
        for (Node node : nodes) {
            int size = node.getContext().getContentServer().getShards().size();
            assertThat("Node has " + size + " shard", size, is(equalTo(2)));
        }

        String indexName = testIndexWith2Shards.getIndexName();
        assertThat("Index is in error", TestUtil.indexHasDeployError(protocol, indexName), is(equalTo(false)));

        protocol.showStructure(false);

        miniCluster.startAdditionalNode();

        assertThat(miniCluster.getStartedNodeCount(), is(equalTo(3)));

//        assertThat("Index is in error", TestUtil.indexHasDeployError(protocol, indexName), is(equalTo(true)));

//        miniCluster.balanceIndex(testIndexWith2Shards.getIndexName());

        assertThat("Index is in error", TestUtil.indexHasDeployError(protocol, indexName), is(equalTo(false)));

        nodes = miniCluster.getNodes();
        for (Node node : nodes) {
            int size = node.getContext().getContentServer().getShards().size();
            assertThat("Node has " + size + " shard", size, is(equalTo(2)));
        }

        protocol.showStructure(false);
    }


}
