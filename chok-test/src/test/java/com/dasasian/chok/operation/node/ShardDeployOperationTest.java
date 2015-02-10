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
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.*;

public class ShardDeployOperationTest extends AbstractNodeOperationMockTest {

    @Test
    public void testDeploy() throws Exception {
        ShardDeployOperation operation = new ShardDeployOperation();
        operation.addShard("shard1", "shardPath1");
        operation.addShard("shard2", "shardPath2");

        File shardFolder = new File("shardFolder");
        Map<String, String> shardMD = new HashMap<>();
        Mockito.when(shardManager.installShard((String) Matchers.notNull(), (String) Matchers.notNull())).thenReturn(shardFolder);
        Mockito.when(contentServer.getShardMetaData((String) Matchers.notNull())).thenReturn(shardMD);

        DeployResult result = operation.execute(context);
        InOrder inOrder = inOrder(protocol, shardManager, contentServer);
        for (String shard : operation.getShardNames()) {
            inOrder.verify(shardManager).installShard(shard, operation.getShardPath(shard));
            inOrder.verify(contentServer).addShard(shard, shardFolder);
            inOrder.verify(protocol).publishShard(node, shard);
        }
        assertEquals(0, result.getShardExceptions().size());
        assertEquals(2, result.getShardMetaDataMaps().size());
        System.out.println(result.getShardMetaDataMaps());
        for (String shardName : operation.getShardNames()) {
            assertTrue(result.getShardMetaDataMaps().containsKey(shardName));
        }
    }

    @Test
    public void testDeployWithOneFailingShard() throws Exception {
        ShardDeployOperation operation = new ShardDeployOperation();
        operation.addShard("shard1", "shardPath1");
        operation.addShard("shard2", "shardPath2");

        File shardFolder = new File("shardFolder");
        Map<String, String> shardMD = new HashMap<>();
        Mockito.when(shardManager.installShard((String) Matchers.notNull(), (String) Matchers.notNull())).thenReturn(shardFolder);
        Mockito.when(contentServer.getShardMetaData((String) Matchers.notNull())).thenReturn(shardMD);

        String failingShard = operation.getShardNames().iterator().next();
        Mockito.doThrow(new Exception("testException")).when(contentServer).addShard(failingShard, shardFolder);

        DeployResult result = operation.execute(context);
        InOrder inOrder = inOrder(protocol, shardManager, contentServer);
        for (String shard : operation.getShardNames()) {
            inOrder.verify(shardManager).installShard(shard, operation.getShardPath(shard));
            inOrder.verify(contentServer).addShard(shard, shardFolder);
            if (!shard.equals(failingShard)) {
                inOrder.verify(protocol).publishShard(node, shard);
            }
        }
        assertEquals(1, result.getShardMetaDataMaps().size());
        assertEquals(1, result.getShardExceptions().size());
        assertEquals(failingShard, result.getShardExceptions().entrySet().iterator().next().getKey());
        Mockito.verify(shardManager).uninstallShard(Matchers.eq(failingShard));
    }
}
