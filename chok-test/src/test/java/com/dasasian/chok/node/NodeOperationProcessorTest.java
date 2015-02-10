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
package com.dasasian.chok.node;

import com.dasasian.chok.node.Node.NodeOperationProcessor;
import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.testutil.mockito.SleepingAnswer;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class NodeOperationProcessorTest {

    private NodeQueue _queue = Mockito.mock(NodeQueue.class);
    private NodeContext _context = Mockito.mock(NodeContext.class);
    private Node _node = Mockito.mock(Node.class);
    private NodeOperationProcessor _processor;

    public NodeOperationProcessorTest() throws InterruptedException {
        Mockito.when(_context.getNode()).thenReturn(_node);
        Mockito.when(_node.isRunning()).thenReturn(true);
        Mockito.when(_node.getName()).thenReturn("aNode");
        _processor = new NodeOperationProcessor(_queue, _context);
        // when(queue.peek()).thenAnswer(new SleepingAnswer());
    }

    @Test(timeout = 10000)
    public void testInterruptedException_Queue() throws Exception {
        Mockito.when(_queue.peek()).thenThrow(new InterruptedException());
        _processor.run();
    }

    @Test(timeout = 10000)
    public void testInterruptedException_Queue_Zk() throws Exception {
        Mockito.when(_queue.peek()).thenThrow(new ZkInterruptedException(new InterruptedException()));
        _processor.run();
    }

    @Test(timeout = 10000)
    public void testInterruptedException_Operation() throws Exception {
        NodeOperation nodeOperation = Mockito.mock(NodeOperation.class);
        Mockito.when(_queue.peek()).thenReturn(nodeOperation);
        Mockito.when(nodeOperation.execute(_context)).thenThrow(new InterruptedException());
        _processor.run();
    }

    @Test(timeout = 10000)
    public void testInterruptedException_Operation_ZK() throws Exception {
        NodeOperation nodeOperation = Mockito.mock(NodeOperation.class);
        Mockito.when(_queue.peek()).thenReturn(nodeOperation);
        Mockito.when(nodeOperation.execute(_context)).thenThrow(new ZkInterruptedException(new InterruptedException()));
        _processor.run();
    }

    @Test(timeout = 10000)
    public void testDontStopOnOOM() throws Exception {
        Mockito.when(_queue.peek()).thenThrow(new OutOfMemoryError("test exception")).thenAnswer(new SleepingAnswer());
        Thread thread = new Thread() {
            public void run() {
                _processor.run();
            }

        };
        thread.start();
        Thread.sleep(500);
        assertEquals(true, thread.isAlive());
        thread.interrupt();
        Mockito.verify(_queue, Mockito.atLeast(2)).peek();
    }
}
