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
package com.dasasian.chok.operation.node;

import com.dasasian.chok.node.IContentServer;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.node.NodeContext;
import com.dasasian.chok.node.ShardManager;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.Mocks;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

public class AbstractNodeOperationMockTest extends AbstractTest {

    protected InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
    protected Node node = Mocks.mockNode();
    protected ShardManager shardManager = Mockito.mock(ShardManager.class);
    protected IContentServer contentServer = Mockito.mock(IContentServer.class);
    protected NodeContext context = new NodeContext(protocol, node, shardManager, contentServer);

}
