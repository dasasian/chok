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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class ShardRedeployOperationTest extends AbstractNodeOperationMockTest {

    @Before
    public void setUp() {
        Mockito.when(shardManager.getShardFolder((String) Matchers.notNull())).thenReturn(new File("shardFolder"));
    }

    @Test
    public void testRedeploy() throws Exception {
        List<String> shards = Arrays.asList("shard1", "shard2");
        ShardRedeployOperation operation = new ShardRedeployOperation(shards);
        operation.execute(context);

        InOrder inOrder = inOrder(protocol, contentServer);
        for (String shard : shards) {
            inOrder.verify(contentServer).addShard(Matchers.eq(shard), (File) Matchers.notNull());
            inOrder.verify(protocol).publishShard(eq(node), Matchers.eq(shard));
        }
    }

    @Test
    public void testRedeployShardAlreadyKnownToNodeManaged() throws Exception {
        List<String> shards = Arrays.asList("shard1", "shard2");

        Mockito.when(contentServer.getShards()).thenReturn(shards);
        ShardRedeployOperation operation = new ShardRedeployOperation(shards);
        operation.execute(context);

        // only publish but not add to nodemanaged again
        InOrder inOrder = inOrder(protocol, contentServer);
        for (String shard : shards) {
            inOrder.verify(protocol).publishShard(eq(node), Matchers.eq(shard));
        }
    }
}
