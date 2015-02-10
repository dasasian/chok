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
package com.dasasian.chok.protocol;

import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.testutil.AbstractZkTest;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Matchers.endsWith;
import static org.mockito.Mockito.*;

public class NodeQueueTest extends AbstractZkTest {

    private String getRootPath() {
        // this path is cleaned up by ZkSystem!
        return zk.getZkConfiguration().getRootPath() + "/queue";
    }

    @Test(timeout = 15000)
    public void testResultMechanism() throws Exception {
        final NodeQueue queue = new NodeQueue(zk.getZkClient(), getRootPath());
        NodeOperation nodeOperation = mock(NodeOperation.class);
        String elementId = queue.add(nodeOperation);

        assertNotNull(queue.peek());
        OperationResult result = new OperationResult("ndoe1");
        queue.complete(result);
        assertNotNull(queue.getResult(elementId, false));

        assertNotNull(queue.getResult(elementId, true));
        assertNull(queue.getResult(elementId, true));
    }

    @Test(timeout = 15000)
    public void testResultMechanism_DeletingOldResults() throws Exception {
        final NodeQueue queue = new NodeQueue(zk.getZkClient(), getRootPath());
        String elementId = "operation-0000000000"; // cheat, we know the internals
        assertNull(queue.getResult(elementId, false));
        zk.getZkClient().createPersistent(getRootPath() + "/results/" + elementId, "");
        assertNotNull(queue.getResult(elementId, false));

        NodeOperation nodeOperation = mock(NodeOperation.class);
        queue.add(nodeOperation);
        assertNull(queue.getResult(elementId, false));
    }

    @Test(timeout = 15000)
    public void testResultCleanup() throws Exception {
        ZkClient zkClientSpy = spy(zk.getZkClient());
        NodeQueue queue = new NodeQueue(zkClientSpy, getRootPath());
        NodeOperation nodeOperation = mock(NodeOperation.class);
        String elementName = queue.add(nodeOperation);

        OperationResult result = new OperationResult("node1");
        // cause a unclean state
        doThrow(new IllegalStateException("test exception")).when(zkClientSpy).delete(endsWith(elementName));
        try {
            queue.complete(result);
            verify(zkClientSpy).createEphemeral(endsWith(elementName));
            fail("should throw exception");
        }
        catch (Exception e) {
            // expected
        }

        // we have now both, a operation and the operation for it
        assertEquals(1, queue.getResults().size());
        assertFalse(queue.isEmpty());

        // this should only be possible if zk connection fails so we try to cleanup
        // on queue initialization
        queue = new NodeQueue(zk.getZkClient(), getRootPath());
        assertEquals(1, queue.getResults().size());
        assertTrue(queue.isEmpty());
    }
}
