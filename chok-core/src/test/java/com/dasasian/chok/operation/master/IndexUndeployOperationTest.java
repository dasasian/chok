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
package com.dasasian.chok.operation.master;

import com.dasasian.chok.node.Node;
import com.dasasian.chok.operation.node.ShardUndeployOperation;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.testutil.Mocks;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import org.junit.Test;

import java.util.List;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.*;

public class IndexUndeployOperationTest extends AbstractMasterNodeZkTest {

    @Test
    public void testUndeployIndex() throws Exception {
        // add nodes and index
        List<Node> nodes = Mocks.mockNodes(2);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        deployIndex(nodes, nodeQueues);
        assertNotNull(getInteractionProtocol().getIndexMD(testIndex.getIndexName()));
        assertNull(getInteractionProtocol().getIndexMD(testIndex.getIndexName()).getDeployError());
        assertThat(getInteractionProtocol().getZkClient().getChildren(getZkConf().getPath(PathDef.SHARD_TO_NODES))).isNotEmpty();
        String shardName = getInteractionProtocol().getIndexMD(testIndex.getIndexName()).getShards().iterator().next().getName();

        // undeploy
        IndexUndeployOperation undeployOperation = new IndexUndeployOperation(testIndex.getIndexName());
        undeployOperation.execute(masterContext, EMPTY_LIST);
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(1, nodeqQueue.size());
            assertTrue(nodeqQueue.peek() instanceof ShardUndeployOperation);
        }
        assertNull(getInteractionProtocol().getIndexMD(testIndex.getIndexName()));
        assertEquals(0, getInteractionProtocol().getIndices().size());
        assertThat(getInteractionProtocol().getZkClient().getChildren(getZkConf().getPath(PathDef.SHARD_TO_NODES))).isEmpty();

        // again delete shard-to-node path - see CHOK-178
        getInteractionProtocol().getZkClient().createPersistent(getZkConf().getPath(PathDef.SHARD_TO_NODES, shardName));
        assertThat(getInteractionProtocol().getZkClient().getChildren(getZkConf().getPath(PathDef.SHARD_TO_NODES))).isNotEmpty();
        undeployOperation.nodeOperationsComplete(masterContext, EMPTY_LIST);
        assertThat(getInteractionProtocol().getZkClient().getChildren(getZkConf().getPath(PathDef.SHARD_TO_NODES))).isEmpty();
    }

    @Test
    public void testUndeployErrorIndex() throws Exception {
        // add nodes and index
        List<Node> nodes = Mocks.mockNodes(2);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        deployIndexWithError();
        publishShards(nodes, nodeQueues);
        assertNotNull(getInteractionProtocol().getIndexMD(testIndex.getIndexName()));
        assertNotNull(getInteractionProtocol().getIndexMD(testIndex.getIndexName()).getDeployError());

        // balance the index does not change anything
        IndexUndeployOperation undeployOperation = new IndexUndeployOperation(testIndex.getIndexName());
        undeployOperation.execute(masterContext, EMPTY_LIST);
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(1, nodeqQueue.size());
            assertTrue(nodeqQueue.peek() instanceof ShardUndeployOperation);
        }
        assertNull(getInteractionProtocol().getIndexMD(testIndex.getIndexName()));
        assertEquals(0, getInteractionProtocol().getIndices().size());
    }

}
