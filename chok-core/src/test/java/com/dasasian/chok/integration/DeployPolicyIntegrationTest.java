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
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestServer;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class DeployPolicyIntegrationTest extends AbstractTest {

    @ClassRule
    public static ChokMiniCluster miniCluster = new ChokMiniCluster(SimpleTestServer.class, 2, 20000, TestNodeConfigurationFactory.class);

    public TestIndex testIndexWithOnShard = TestIndex.createTestIndex(temporaryFolder, 1);

    @Test
    public void testEqualDistribution3WhenMoreNodesThenShards() throws Exception {
        int replicationCount = 1;
        miniCluster.deployTestIndexes(testIndexWithOnShard.getIndexFile(), replicationCount);

        final InteractionProtocol protocol = miniCluster.getProtocol();
        // todo this could be better
        assertTrue("node count=" + miniCluster.getRunningNodeCount() + " not greater than number of indices " + protocol.getIndices().size(), miniCluster.getRunningNodeCount() > protocol.getIndices().size());

        protocol.showStructure(false);
        List<Node> nodes = miniCluster.getNodes();
        for (Node node : nodes) {
            assertTrue("Node has " + node.getContext().getContentServer().getShards().size() + " shard", 1 >= node.getContext().getContentServer().getShards().size());
        }
    }
}
