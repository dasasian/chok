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

import com.dasasian.chok.node.Node;
import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.master.AbstractMasterNodeZkTest;
import com.dasasian.chok.operation.master.MasterOperation;
import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.testutil.Mocks;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class OperationRegistryTest extends AbstractMasterNodeZkTest {

    private final List<Node> nodes = Mocks.mockNodes(5);
    private final MasterOperation masterOperation = mock(MasterOperation.class);

    private OperationRegistry operationRegistry;

    @Before
    public void setOperationRegistry() {
        operationRegistry = new OperationRegistry(masterContext);
    }

    @Test(timeout = 10000)
    public void testAllOperationsDoneWithoutResults() throws Exception {
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        OperationWatchdog operationWatchdog = beginMasterOperation(nodes, masterOperation);

        // execute node operations
        List<OperationResult> operationResults = new ArrayList<>();
        for (NodeQueue nodeQueue : nodeQueues) {
            NodeOperation nodeOperation = nodeQueue.peek();
            assertNotNull(nodeOperation);
            nodeQueue.remove();
            operationResults.add(null);
        }
        operationWatchdog.join();
        assertTrue(operationWatchdog.isDone());
        assertEquals(0, operationWatchdog.getOpenOperationCount());
        verify(masterOperation, times(1)).nodeOperationsComplete(masterContext, operationResults);
    }

    @Test(timeout = 10000)
    public void testAllOperationsDone() throws Exception {
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        OperationWatchdog operationWatchdog = beginMasterOperation(nodes, masterOperation);

        // execute node operations
        List<OperationResult> operationResults = new ArrayList<>();
        int i = 0;
        for (NodeQueue nodeQueue : nodeQueues) {
            NodeOperation nodeOperation = nodeQueue.peek();
            assertNotNull(nodeOperation);
            OperationResult result = new OperationResult(nodes.get(i).getName());
            operationResults.add(result);
            nodeQueue.complete(result);
            i++;
        }
        operationWatchdog.join();
        assertTrue(operationWatchdog.isDone());
        assertEquals(0, operationWatchdog.getOpenOperationCount());
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);

        verify(masterOperation, times(1)).nodeOperationsComplete(eq(masterContext), argument.capture());
        List<OperationResult> capturedResults = argument.getValue();
        assertEquals(operationResults.size(), capturedResults.size());
        for (OperationResult result : capturedResults) {
            assertNotNull(result);
            assertNotNull(result.getNodeName());
        }
    }

    @Test(timeout = 10000)
    public void testOneOperationsMissing() throws Exception {
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        OperationWatchdog watchdog = beginMasterOperation(nodes, masterOperation);

        // execute operation
        nodeQueues.get(0).remove();
        nodeQueues.get(1).remove();
        nodeQueues.get(2).remove();

        // deployment be still pending
        watchdog.join(250);
        assertFalse(watchdog.isDone());
        assertEquals(2, watchdog.getOpenOperationCount());
        watchdog.cancel();
    }

    @Test(timeout = 10000)
    public void testOperationsDoneOrNodeGone() throws Exception {
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);
        OperationWatchdog watchdog = beginMasterOperation(nodes, masterOperation);

        nodeQueues.get(0).remove();
        nodeQueues.get(1).remove();
        nodeQueues.get(2).remove();

        getInteractionProtocol().unregisterComponent(nodes.get(3));
        getInteractionProtocol().unregisterComponent(nodes.get(4));

        // deployment should be complete
        watchdog.join();
        assertTrue(watchdog.isDone());
        assertEquals(0, watchdog.getOpenOperationCount());
    }

    @Test(timeout = 10000)
    public void testGetRunningOperations() throws Exception {
        Mocks.publishNodes(getInteractionProtocol(), nodes);
        beginMasterOperation(nodes, masterOperation);
        assertEquals(1, operationRegistry.getRunningOperations().size());
        operationRegistry.shutdown();
    }

    private OperationWatchdog beginMasterOperation(List<Node> nodes, MasterOperation operation) {
        List<OperationId> operationIds = new ArrayList<>();
        for (Node node : nodes) {
            OperationId operationId = getInteractionProtocol().addNodeOperation(node.getName(), mock(NodeOperation.class));
            operationIds.add(operationId);
        }
        OperationWatchdog watchdog = new OperationWatchdog("id", operation, operationIds);
        operationRegistry.watchFor(watchdog);
        assertFalse(watchdog.isDone());
        assertEquals(nodes.size(), watchdog.getOpenOperationCount());
        return watchdog;
    }
}
