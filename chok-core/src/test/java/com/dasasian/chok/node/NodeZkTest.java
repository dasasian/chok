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

import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.testutil.AbstractZkTest;
import com.dasasian.chok.testutil.NodeConfigurationFactory;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.mockito.SerializableCountDownLatchAnswer;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestServer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NodeZkTest extends AbstractZkTest {

    private final NodeConfigurationFactory nodeConfigurationFactory = new TestNodeConfigurationFactory(temporaryFolder);

    @Test
    public void testShutdown_shouldCleanupZkClientSubscriptions() {
        int numberOfListeners = zk.getZkClient().numberOfListeners();
        Node node = new Node(protocol, nodeConfigurationFactory.getConfiguration(), new SimpleTestServer());
        node.start();
        node.shutdown();
        assertEquals(numberOfListeners, zk.getZkClient().numberOfListeners());
    }

    @Test(timeout = 10000)
    public void testNodeOperationPickup() throws Exception {
        Node node = new Node(protocol, nodeConfigurationFactory.getConfiguration(), new SimpleTestServer());
        node.start();


        SerializableCountDownLatchAnswer answer = new SerializableCountDownLatchAnswer(2);

//        NodeOperation operation1 = mock(NodeOperation.class, withSettings().serializable());
//        NodeOperation operation2 = mock(NodeOperation.class, withSettings().serializable());
//        when(operation1.execute((NodeContext) notNull())).thenAnswer(answer);
//        when(operation2.execute((NodeContext) notNull())).thenAnswer(answer);

        NodeOperation operation1 = new TestNodeOperation(answer);
        NodeOperation operation2 = new TestNodeOperation(answer);

        protocol.addNodeOperation(node.getName(), operation1);
        protocol.addNodeOperation(node.getName(), operation2);

        answer.getCountDownLatch().await();

        node.shutdown();
    }

    @Test(timeout = 20000)
    public void testNodeOperationPickup_AfterReconnect() throws Exception {
        Node node = new Node(protocol, nodeConfigurationFactory.getConfiguration(), new SimpleTestServer());
        node.start();

        node.disconnect();
        node.reconnect();

        SerializableCountDownLatchAnswer answer = new SerializableCountDownLatchAnswer(2);

//        NodeOperation operation1 = mock(NodeOperation.class, withSettings().serializable());
//        NodeOperation operation2 = mock(NodeOperation.class, withSettings().serializable());
//        when(operation1.execute((NodeContext) notNull())).thenAnswer(answer);
//        when(operation2.execute((NodeContext) notNull())).thenAnswer(answer);

        NodeOperation operation1 = new TestNodeOperation(answer);
        NodeOperation operation2 = new TestNodeOperation(answer);

        protocol.addNodeOperation(node.getName(), operation1);
        protocol.addNodeOperation(node.getName(), operation2);
        answer.getCountDownLatch().await();

        node.shutdown();
    }

    @Test(timeout = 2000000)
    public void testNodeReconnectWithInterruptSwallowingOperation() throws Exception {
        Node node = new Node(protocol, nodeConfigurationFactory.getConfiguration(), new SimpleTestServer());
        node.start();
        protocol.addNodeOperation(node.getName(), new InterruptSwallowingOperation());
        Thread.sleep(200);
        node.disconnect();
        node.reconnect();

        node.shutdown();
    }

    @Test(timeout = 10000)
    public void testNodeOperationException() throws Exception {
        Node node = new Node(protocol, nodeConfigurationFactory.getConfiguration(), new SimpleTestServer());
        node.start();

        SerializableCountDownLatchAnswer answer = new SerializableCountDownLatchAnswer(2);

//        NodeOperation operation1 = mock(NodeOperation.class, withSettings().serializable());
//        NodeOperation operation2 = mock(NodeOperation.class, withSettings().serializable());
//        when(operation1.execute((NodeContext) notNull())).thenAnswer(answer);
//        when(operation2.execute((NodeContext) notNull())).thenAnswer(answer);

        NodeOperation operation1 = new TestNodeOperation(answer);
        NodeOperation operation2 = new TestNodeOperation(answer);

        protocol.addNodeOperation(node.getName(), operation1);
        protocol.addNodeOperation(node.getName(), operation2);
        answer.getCountDownLatch().await();

        node.shutdown();
    }

    private static class InterruptSwallowingOperation implements NodeOperation {

        private static final long serialVersionUID = 1L;

        @Override
        public OperationResult execute(NodeContext context) throws InterruptedException {
            try {
                System.out.println("NodeZkTest.InterruptSwallowingOperation.execute()- entering sleep");
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                System.out.println("NodeZkTest.InterruptSwallowingOperation.execute()- leaving sleep");
            }
            return null;
        }

    }

}
