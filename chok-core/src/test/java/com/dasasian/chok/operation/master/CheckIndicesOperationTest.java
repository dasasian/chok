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
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.testutil.Mocks;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CheckIndicesOperationTest extends AbstractMasterNodeZkTest {

    @Test
    public void testBalanceUnderreplicatedIndex() throws Exception {
        MasterQueue masterQueue = getInteractionProtocol().publishMaster(mockMaster);

        // add nodes and index
        List<Node> nodes = Mocks.mockNodes(2);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        deployIndex(nodes, nodeQueues);
        assertEquals(0, masterQueue.size());

        // balance the index does not change anything
        CheckIndicesOperation checkOperation = new CheckIndicesOperation();
        checkOperation.execute(masterContext, EMPTY_LIST);
        assertEquals(0, masterQueue.size());

        // add node and then balance again
        Node node3 = Mocks.mockNode();
        Mocks.publishNode(getInteractionProtocol(), node3);
        checkOperation.execute(masterContext, EMPTY_LIST);
        assertEquals(1, masterQueue.size());
    }

    @Test
    public void testBalanceOverreplicatedIndex() throws Exception {
        MasterQueue masterQueue = getInteractionProtocol().publishMaster(mockMaster);

        // add nodes and index
        List<Node> nodes = Mocks.mockNodes(3);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        deployIndex(nodes, nodeQueues);
        assertEquals(0, masterQueue.size());

        // balance the index does not change anything
        CheckIndicesOperation balanceOperation = new CheckIndicesOperation();
        balanceOperation.execute(masterContext, EMPTY_LIST);
        assertEquals(0, masterQueue.size());

        // decrease the replication count and then balance again
        IndexMetaData indexMD = getInteractionProtocol().getIndexMD(testIndex.getIndexName());
        indexMD.setReplicationLevel(2);
        getInteractionProtocol().updateIndexMD(indexMD);
        balanceOperation.execute(masterContext, EMPTY_LIST);
        assertEquals(1, masterQueue.size());
    }

}