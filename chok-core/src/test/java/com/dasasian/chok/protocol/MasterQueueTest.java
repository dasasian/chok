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

import com.dasasian.chok.master.MasterContext;
import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.master.MasterOperation;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.testutil.AbstractZkTest;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.endsWith;
import static org.mockito.Mockito.*;

public class MasterQueueTest extends AbstractZkTest {

    private String getRootPath() {
        // this path is cleaned up by ZkSystem!
        return zk.getZkConfiguration().getRootPath() + "/queue";
    }

    @Test(timeout = 15000)
    public void testWatchDogMechanism() throws Exception {
        final MasterQueue queue = new MasterQueue(zk.getZkClient(), getRootPath());
        MasterOperation masterOperation = mock(MasterOperation.class);
        queue.add(masterOperation);

        List<OperationId> operationIds = new ArrayList<>();
        operationIds.add(new OperationId("node1", "e1"));

        queue.moveOperationToWatching(masterOperation, operationIds);
        assertTrue(queue.isEmpty());
        assertEquals(1, queue.getWatchdogs().size());
    }

    @Test(timeout = 15000)
    public void testKeepStateBetweenExecutionAndCompletion() throws Exception {
        final MasterQueue queue = new MasterQueue(zk.getZkClient(), getRootPath());

        MasterOperation masterOperation = new StatefulMasterOperation();
        queue.add(masterOperation);

        masterOperation = queue.peek();
        masterOperation.execute(mock(MasterContext.class), Collections.EMPTY_LIST);

        queue.moveOperationToWatching(masterOperation, Collections.EMPTY_LIST);
        assertEquals(1, queue.getWatchdogs().size());
        masterOperation = queue.getWatchdogs().get(0).getOperation();
        masterOperation.nodeOperationsComplete(mock(MasterContext.class), Collections.EMPTY_LIST);
    }

    @Test(timeout = 15000)
    public void testWatchDogCleanup() throws Exception {
        ZkClient zkClientSpy = spy(zk.getZkClient());
        MasterQueue queue = new MasterQueue(zkClientSpy, getRootPath());
        MasterOperation masterOperation = mock(MasterOperation.class);
        String elementName = queue.add(masterOperation);

        List<OperationId> operationIds = new ArrayList<>();
        operationIds.add(new OperationId("node1", "e1"));

        // cause a unclean state
        doThrow(new IllegalStateException("test exception")).when(zkClientSpy).delete(endsWith(elementName));
        try {
            queue.moveOperationToWatching(masterOperation, operationIds);
            verify(zkClientSpy).delete(endsWith(elementName));
            fail("should throw exception");
        } catch (Exception e) {
            // expected
        }

        // we have now both, a operation and the operation for it
        assertEquals(1, queue.getWatchdogs().size());
        assertFalse(queue.isEmpty());

        // this should only be possible if zk connection fails so we try to cleanup
        // on queue initialization
        queue = new MasterQueue(zk.getZkClient(), getRootPath());
        assertEquals(1, queue.getWatchdogs().size());
        assertTrue(queue.isEmpty());
    }

    static class StatefulMasterOperation implements MasterOperation {
        private static final long serialVersionUID = 1L;
        private boolean _executed = false;

        @Override
        public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
            _executed = true;
            return null;
        }

        @Override
        public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
            return ExecutionInstruction.EXECUTE;
        }

        @Override
        public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
            assertTrue(_executed);
        }
    }

}
