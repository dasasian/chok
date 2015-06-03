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
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.protocol.metadata.NodeMetaData;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.NodeConfigurationFactory;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.mockito.SleepingAnswer;
import com.dasasian.chok.util.NodeConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NodeMockTest extends AbstractTest {

    public static TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 1);
    private final NodeConfigurationFactory nodeConfigurationFactory = new TestNodeConfigurationFactory(temporaryFolder);
    private final NodeConfiguration testConfiguration = nodeConfigurationFactory.getConfiguration();
    private InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
    private IContentServer contentServer = Mockito.mock(IContentServer.class);
    private Node node = new Node(protocol, testConfiguration, contentServer);
    private NodeQueue queue = Mockito.mock(NodeQueue.class);

    @Before
    public void setUp() throws IOException {
        Mockito.when(protocol.publishNode(Matchers.eq(node), (NodeMetaData) Matchers.notNull())).thenReturn(queue);
    }

    @Test
    public void testGracefulStartup_Shutdown() throws Exception {
        NodeOperation nodeOperation = Mockito.mock(NodeOperation.class);
        Mockito.when(queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());

        node.start();

        assertNotNull(node.getName());
        assertNotNull(node.getRpcServer());
        assertTrue(node.getRPCServerPort() > 0);
        Mockito.verify(contentServer).init((String) Matchers.notNull(), (NodeConfiguration) Matchers.notNull());
        Thread.sleep(200);
        Mockito.verify(nodeOperation).execute((NodeContext) Matchers.notNull());

        node.shutdown();
        Mockito.verify(protocol).unregisterComponent(node);
        Mockito.verify(contentServer).shutdown();
    }

    @Test
    public void testDisconnectReconnect() throws Exception {
        NodeOperation nodeOperation = Mockito.mock(NodeOperation.class);
        Mockito.when(queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());
        node.start();
        Thread.sleep(200);
        Mockito.verify(nodeOperation, Mockito.times(1)).execute((NodeContext) Matchers.notNull());

        node.disconnect();
        NodeOperation nodeOperation2 = Mockito.mock(NodeOperation.class);
        Mockito.reset(queue);
        Mockito.when(queue.peek()).thenReturn(nodeOperation2).thenAnswer(new SleepingAnswer());

        node.reconnect();
        Thread.sleep(200);
        Mockito.verify(nodeOperation, Mockito.times(1)).execute((NodeContext) Matchers.notNull());
        Mockito.verify(nodeOperation2, Mockito.times(1)).execute((NodeContext) Matchers.notNull());
        node.shutdown();
    }

    @Test
    public void testShutdown_doesNotCloseZkClient() throws Exception {
        Mockito.when(queue.peek()).thenAnswer(new SleepingAnswer());
        node.start();
        node.shutdown();
        Mockito.verify(protocol, Mockito.never()).disconnect();
    }

    @Test
    public void testRedployInstalledShards() throws Exception {
        NodeOperation nodeOperation = Mockito.mock(NodeOperation.class);
        Mockito.when(queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());

        // start and add shard
        node.start();
        Mockito.verify(contentServer, Mockito.times(0)).addShard(Matchers.anyString(), Matchers.any(File.class));

        File shardFile = testIndex.getShardFiles().get(0);

        String shardName = shardFile.getName();
        node.getContext().getShardManager().installShard(shardName, shardFile.getAbsolutePath());

        // restart, node should be added
        node.shutdown();

        node = new Node(protocol, testConfiguration, contentServer);
        Mockito.when(protocol.publishNode(Matchers.eq(node), (NodeMetaData) Matchers.notNull())).thenReturn(queue);
        node.start();
        Mockito.verify(contentServer, Mockito.times(1)).addShard(Matchers.anyString(), Matchers.any(File.class));
        Mockito.verify(protocol).publishShard(Matchers.eq(node), Matchers.eq(shardName));
        node.shutdown();
    }

}
