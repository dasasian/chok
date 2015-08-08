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

public class ShardReloadOperationTest extends AbstractNodeOperationMockTest {

    @Test
    public void testReload() throws Exception {
        final String shard1name = "shard1";
        final URI shard1Uri = new URI("file:///tmp/shard1Folder");
        final Path localShardPath1 = Paths.get("path1");
        final Path oldLocalShardPath1 = Paths.get("oldpath1");

        Mockito.when(shardManager.installShard(Matchers.anyString(), Matchers.any(URI.class), Matchers.anyBoolean())).thenReturn(localShardPath1);
        Mockito.when(contentServer.replaceShard(Matchers.anyString(), Matchers.any(Path.class))).thenReturn(oldLocalShardPath1);

        Map<String, URI> shards = ImmutableMap.of(shard1name, shard1Uri); //, "shard2", new URI("file:///tmp/shard2Folder"));
        ShardReloadOperation operation = new ShardReloadOperation(shards);
        operation.execute(context);

        InOrder inOrder = inOrder(protocol, contentServer, shardManager);
        for (String shard : shards.keySet()) {
            inOrder.verify(shardManager).installShard(Matchers.eq(shard), Matchers.eq(shard1Uri), Matchers.eq(false));
            inOrder.verify(contentServer).replaceShard(Matchers.eq(shard), Matchers.eq(localShardPath1));
            inOrder.verify(shardManager).removeLocalShardFolder(Matchers.eq(oldLocalShardPath1));
            inOrder.verify(protocol).publishShard(eq(node), Matchers.eq(shard));
        }
    }

}
