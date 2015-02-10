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
package com.dasasian.chok.operation.master;

import com.dasasian.chok.master.DefaultDistributionPolicy;
import com.dasasian.chok.master.MasterContext;
import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.operation.node.ShardUndeployOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.testutil.Mocks;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.*;

public class RemoveObsoleteShardsOperationTest {

    protected static final List EMPTY_LIST = Collections.EMPTY_LIST;

    @Test
    public void testMockRemove() throws Exception {
        String nodeName = "nodeA";
        String someOldShard = AbstractIndexOperation.createShardName("someOldIndex", "someOldShard");
        InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
        MasterQueue queue = Mockito.mock(MasterQueue.class);
        MasterContext context = new MasterContext(protocol, Mocks.mockMaster(), new DefaultDistributionPolicy(), queue);
        Mockito.when(protocol.getNodeShards(nodeName)).thenReturn(Arrays.asList(someOldShard));

        RemoveObsoleteShardsOperation operation = new RemoveObsoleteShardsOperation(nodeName);
        operation.execute(context, EMPTY_LIST);

        ArgumentCaptor<NodeOperation> captor = ArgumentCaptor.forClass(NodeOperation.class);
        Mockito.verify(protocol).addNodeOperation(Matchers.eq(nodeName), captor.capture());
        assertThat(captor.getValue(), instanceOf(ShardUndeployOperation.class));
        ShardUndeployOperation undeployOperation = (ShardUndeployOperation) captor.getValue();
        assertEquals(1, undeployOperation.getShardNames().size());
        assertEquals(someOldShard, undeployOperation.getShardNames().iterator().next());
    }

    @Test
    public void testNotRemoveDeployingIndex() throws Exception {
        String nodeName = "nodeA";
        String indexName = "someOldIndex";
        String someOldShard = AbstractIndexOperation.createShardName(indexName, "someOldShard");
        InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
        MasterQueue queue = Mockito.mock(MasterQueue.class);
        MasterContext context = new MasterContext(protocol, Mocks.mockMaster(), new DefaultDistributionPolicy(), queue);
        Mockito.when(protocol.getNodeShards(nodeName)).thenReturn(Arrays.asList(someOldShard));

        RemoveObsoleteShardsOperation operation = new RemoveObsoleteShardsOperation(nodeName);
        operation.execute(context, new ArrayList<MasterOperation>(Arrays.asList(new IndexDeployOperation(indexName, "path", 1))));

        Mockito.verify(protocol, Mockito.times(0)).addNodeOperation(Matchers.eq(nodeName), (NodeOperation) Matchers.notNull());
    }
}
