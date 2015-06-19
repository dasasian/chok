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

import com.dasasian.chok.master.MasterContext;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.master.MasterOperation.ExecutionInstruction;
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.testutil.Mocks;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.HDFSChokFileSystem;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class BalanceIndexOperationTest extends AbstractMasterNodeZkTest {

    @Test
    public void testGetExecutionInstruction() throws Exception {
        // only lock operations on same index
        MasterOperation op1 = new BalanceIndexOperation("index1");
        MasterOperation op2 = new BalanceIndexOperation("index1");

        assertEquals(ExecutionInstruction.EXECUTE, op1.getExecutionInstruction(EMPTY_LIST));
        assertEquals(ExecutionInstruction.CANCEL, op2.getExecutionInstruction(Arrays.asList(op1)));
    }

    @Test
    public void testBalanceUnderreplicatedIndex() throws Exception {
        // add nodes and index
        List<Node> nodes = Mocks.mockNodes(2);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        deployIndexWithError();

        // index deployed on 2 nodes / desired replica is 3
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(1, nodeqQueue.size());
        }
        publishShards(nodes, nodeQueues);

        // balance the index does not change anything
        BalanceIndexOperation balanceOperation = new BalanceIndexOperation(testIndex.getIndexName());
        balanceOperation.execute(masterContext, EMPTY_LIST);
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(0, nodeqQueue.size());
        }

        // add node and then balance again
        Node node3 = Mocks.mockNode();
        NodeQueue nodeQueue3 = Mocks.publishNode(getInteractionProtocol(), node3);
        assertEquals(0, nodeQueue3.size());

        balanceOperation.execute(masterContext, EMPTY_LIST);
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(0, nodeqQueue.size());
        }
        assertEquals(1, nodeQueue3.size());
    }

    @Test
    public void testBalanceOverreplicatedIndex() throws Exception {
        // add nodes and index
        List<Node> nodes = Mocks.mockNodes(3);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        deployIndexWithError();
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(1, nodeqQueue.size());
        }

        // publish shards
        publishShards(nodes, nodeQueues);

        // balance the index does not change anything
        BalanceIndexOperation balanceOperation = new BalanceIndexOperation(testIndex.getIndexName());
        balanceOperation.execute(masterContext, EMPTY_LIST);
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(0, nodeqQueue.size());
        }

        // decrease the replication count and then balance again
        IndexMetaData indexMD = getInteractionProtocol().getIndexMD(testIndex.getIndexName());
        indexMD.setReplicationLevel(2);
        getInteractionProtocol().updateIndexMD(indexMD);
        balanceOperation.execute(masterContext, EMPTY_LIST);
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(1, nodeqQueue.size());
        }
    }

    @Test
    public void testUnbalancedIndexAfterBalancingIndex() throws Exception {
        // add nodes and index
        List<Node> nodes = Mocks.mockNodes(2);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        deployIndexWithError();

        // index deployed on 2 nodes / desired replica is 3
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(1, nodeqQueue.size());
        }
        publishShards(nodes, nodeQueues);

        // balance the index does not change anything
        BalanceIndexOperation balanceOperation = new BalanceIndexOperation(testIndex.getIndexName());
        balanceOperation.execute(masterContext, EMPTY_LIST);
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(0, nodeqQueue.size());
        }

        // node completion does not add another balance op since not enough nodes
        // are there
        MasterQueue masterQueue = getInteractionProtocol().publishMaster(mockMaster);
        assertEquals(0, masterQueue.size());
        balanceOperation.nodeOperationsComplete(masterContext, Collections.EMPTY_LIST);
        assertEquals(0, masterQueue.size());

        // add node and now the balance op should add itself for retry
        Node node3 = Mocks.mockNode();
        NodeQueue nodeQueue3 = Mocks.publishNode(getInteractionProtocol(), node3);
        balanceOperation.nodeOperationsComplete(masterContext, Collections.EMPTY_LIST);
        assertEquals(1, masterQueue.size());

        // now do the balance
        assertEquals(0, nodeQueue3.size());
        balanceOperation.execute(masterContext, EMPTY_LIST);
        assertEquals(1, nodeQueue3.size());
        publishShard(node3, nodeQueue3);

        // now it shouldn't add itself again since the index is balanced
        balanceOperation.nodeOperationsComplete(masterContext, Collections.EMPTY_LIST);
        assertEquals(1, masterQueue.size());
    }

    @Test
    public void testBalanceErrorIndex() throws Exception {
        // add nodes and index
        List<Node> nodes = Mocks.mockNodes(2);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        deployIndexWithError();
        assertTrue(getInteractionProtocol().getIndexMD(testIndex.getIndexName()).hasDeployError());
        assertNotNull(getInteractionProtocol().getIndexMD(testIndex.getIndexName()).getDeployError());

        // balance the index should remove the error
        publishShards(nodes, nodeQueues);
        BalanceIndexOperation balanceOperation = new BalanceIndexOperation(testIndex.getIndexName());
        balanceOperation.execute(masterContext, EMPTY_LIST);
        balanceOperation.nodeOperationsComplete(masterContext, Collections.EMPTY_LIST);
        assertNull(getInteractionProtocol().getIndexMD(testIndex.getIndexName()).getDeployError());
    }

    @Test
    public void testStopBalance_WhenSourceFileDoesNotExistAnymore() throws Exception {
        // add nodes and index
        List<Node> nodes = Mocks.mockNodes(2);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        deployIndexWithError();

        // index deployed on 2 nodes / desired replica is 3
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(1, nodeqQueue.size());
        }
        publishShards(nodes, nodeQueues);

        // add node and then balance again
        Node node3 = Mocks.mockNode();
        NodeQueue nodeQueue3 = Mocks.publishNode(getInteractionProtocol(), node3);
        assertEquals(0, nodeQueue3.size());
        BalanceIndexOperation balanceOperation = new BalanceIndexOperation(testIndex.getIndexName());
        ChokFileSystem fileSystem = Mockito.mock(ChokFileSystem.class);
        Mockito.when(fileSystem.exists(Matchers.any(URI.class))).thenReturn(false);
        MasterContext spiedContext = spy(masterContext);
        Mockito.doReturn(fileSystem).when(spiedContext).getChokFileSystem(Matchers.any(IndexMetaData.class));
        List<OperationId> nodeOperations = balanceOperation.execute(spiedContext, EMPTY_LIST);
        assertEquals(null, nodeOperations);
    }

    @Test
    public void testStopBalance_CantAccessSourceFile() throws Exception {
        // add nodes and index
        List<Node> nodes = Mocks.mockNodes(2);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        deployIndexWithError();

        // index deployed on 2 nodes / desired replica is 3
        for (NodeQueue nodeqQueue : nodeQueues) {
            assertEquals(1, nodeqQueue.size());
        }
        publishShards(nodes, nodeQueues);

        // add node and then balance again
        Node node3 = Mocks.mockNode();
        NodeQueue nodeQueue3 = Mocks.publishNode(getInteractionProtocol(), node3);
        assertEquals(0, nodeQueue3.size());
        BalanceIndexOperation balanceOperation = new BalanceIndexOperation(testIndex.getIndexName());
        MasterContext spiedContext = spy(masterContext);
        Mockito.doThrow(new RuntimeException("test-exception")).when(spiedContext).getChokFileSystem(Matchers.any(IndexMetaData.class));
        List<OperationId> nodeOperations = balanceOperation.execute(spiedContext, EMPTY_LIST);
        assertEquals(null, nodeOperations);
    }

}
