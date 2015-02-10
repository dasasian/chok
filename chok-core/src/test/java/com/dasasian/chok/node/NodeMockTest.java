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

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.*;

public class NodeMockTest extends AbstractTest {

    public static TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 1);
    private final NodeConfigurationFactory nodeConfigurationFactory = new TestNodeConfigurationFactory(temporaryFolder);
    private final NodeConfiguration testConfiguration = nodeConfigurationFactory.getConfiguration();
    private InteractionProtocol protocol = mock(InteractionProtocol.class);
    private IContentServer contentServer = mock(IContentServer.class);
    private Node node = new Node(protocol, testConfiguration, contentServer);
    private NodeQueue queue = mock(NodeQueue.class);

    @Before
    public void setUp() throws IOException {
        when(protocol.publishNode(eq(node), (NodeMetaData) notNull())).thenReturn(queue);
    }

    @Test
    public void testGracefulStartup_Shutdown() throws Exception {
        NodeOperation nodeOperation = mock(NodeOperation.class);
        when(queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());

        node.start();

        assertNotNull(node.getName());
        assertNotNull(node.getRpcServer());
        assertTrue(node.getRPCServerPort() > 0);
        verify(contentServer).init((String) notNull(), (NodeConfiguration) notNull());
        Thread.sleep(200);
        verify(nodeOperation).execute((NodeContext) notNull());

        node.shutdown();
        verify(protocol).unregisterComponent(node);
        verify(contentServer).shutdown();
    }

    @Test
    public void testDisconnectReconnect() throws Exception {
        NodeOperation nodeOperation = mock(NodeOperation.class);
        when(queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());
        node.start();
        Thread.sleep(200);
        verify(nodeOperation, times(1)).execute((NodeContext) notNull());

        node.disconnect();
        NodeOperation nodeOperation2 = mock(NodeOperation.class);
        reset(queue);
        when(queue.peek()).thenReturn(nodeOperation2).thenAnswer(new SleepingAnswer());

        node.reconnect();
        Thread.sleep(200);
        verify(nodeOperation, times(1)).execute((NodeContext) notNull());
        verify(nodeOperation2, times(1)).execute((NodeContext) notNull());
        node.shutdown();
    }

    @Test
    public void testShutdown_doesNotCloseZkClient() throws Exception {
        when(queue.peek()).thenAnswer(new SleepingAnswer());
        node.start();
        node.shutdown();
        verify(protocol, never()).disconnect();
    }

    @Test
    public void testRedployInstalledShards() throws Exception {
        NodeOperation nodeOperation = mock(NodeOperation.class);
        when(queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());

        // start and add shard
        node.start();
        verify(contentServer, times(0)).addShard(anyString(), any(File.class));

        File shardFile = testIndex.getShardFiles().get(0);

        String shardName = shardFile.getName();
        node.getContext().getShardManager().installShard(shardName, shardFile.getAbsolutePath());

        // restart, node should be added
        node.shutdown();

        node = new Node(protocol, testConfiguration, contentServer);
        when(protocol.publishNode(eq(node), (NodeMetaData) notNull())).thenReturn(queue);
        node.start();
        verify(contentServer, times(1)).addShard(anyString(), any(File.class));
        verify(protocol).publishShard(eq(node), eq(shardName));
        node.shutdown();
    }

}
