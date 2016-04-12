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

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;

public class ShardRedeployOperationTest extends AbstractNodeOperationMockTest {

    @Before
    public void setUp() {
        Mockito.when(shardManager.getShardFolder((String) Matchers.notNull(), Matchers.anyLong())).thenReturn(Paths.get("shardFolder"));
    }

    @Test
    public void testRedeploy() throws Exception {
        Map<String, URI> shards = ImmutableMap.of("shard1", new URI("file:///tmp/shardFolder"), "shard2", new URI("file:///tmp/shardFolder"));
        ShardRedeployOperation operation = new ShardRedeployOperation(shards);
        operation.execute(context);

        InOrder inOrder = inOrder(protocol, contentServer);
        for (String shard : shards.keySet()) {
            inOrder.verify(contentServer).addShard(Matchers.eq(shard), (Path) Matchers.notNull());
            inOrder.verify(protocol).publishShard(eq(node), Matchers.eq(shard));
        }
    }

    @Test
    public void testRedeployShardAlreadyKnownToNodeManaged() throws Exception {
        Map<String, URI> shards = ImmutableMap.of("shard1", new URI("file:///tmp/shardFolder"), "shard2", new URI("file:///tmp/shardFolder"));
        Mockito.when(contentServer.getShards()).thenReturn(shards.keySet());
        ShardRedeployOperation operation = new ShardRedeployOperation(shards);
        operation.execute(context);

        // only publish but not add to nodemanaged again
        InOrder inOrder = inOrder(protocol, contentServer);
        for (String shard : shards.keySet()) {
            inOrder.verify(protocol).publishShard(eq(node), Matchers.eq(shard));
        }
    }
}
