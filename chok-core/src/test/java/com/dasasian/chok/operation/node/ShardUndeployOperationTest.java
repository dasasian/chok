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

import org.junit.Test;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;

public class ShardUndeployOperationTest extends AbstractNodeOperationMockTest {

    @Test
    public void testUneploy() throws Exception {
        List<String> shards = Arrays.asList("shard1", "shard2");
        ShardUndeployOperation operation = new ShardUndeployOperation(shards);

        DeployResult result = operation.execute(context);
        InOrder inOrder = inOrder(protocol, shardManager, contentServer);
        for (String shard : operation.getShardNames()) {
            inOrder.verify(protocol).unpublishShard(node, shard);
            inOrder.verify(contentServer).removeShard(shard);
            inOrder.verify(shardManager).uninstallShard(shard);
        }
        assertEquals(0, result.getShardExceptions().size());
    }

    @Test
    public void testUneployWithError() throws Exception {
        List<String> shards = Arrays.asList("shard1", "shard2");
        ShardUndeployOperation operation = new ShardUndeployOperation(shards);

        String failingShard = shards.get(0);
        doThrow(new Exception("testException")).when(contentServer).removeShard(failingShard);

        DeployResult result = operation.execute(context);
        InOrder inOrder = inOrder(protocol, shardManager, contentServer);
        for (String shard : operation.getShardNames()) {
            inOrder.verify(protocol).unpublishShard(node, shard);
            inOrder.verify(contentServer).removeShard(shard);
            inOrder.verify(shardManager).uninstallShard(shard);
        }
        assertEquals(1, result.getShardExceptions().size());
    }

}
