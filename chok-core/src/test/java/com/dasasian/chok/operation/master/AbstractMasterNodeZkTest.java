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
import com.dasasian.chok.master.Master;
import com.dasasian.chok.master.MasterContext;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.operation.node.ShardDeployOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.testutil.AbstractZkTest;
import com.dasasian.chok.testutil.Mocks;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.util.ZkConfiguration;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class AbstractMasterNodeZkTest extends AbstractZkTest {

    protected static final List EMPTY_LIST = Collections.EMPTY_LIST;

    protected final TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 4);
    public MasterContext masterContext;
    protected Master mockMaster;

    @Before
    public void setMasterContext() {
        mockMaster = Mocks.mockMaster();
        masterContext = new MasterContext(protocol, mockMaster, new DefaultDistributionPolicy(), protocol.publishMaster(mockMaster));
    }

    protected ZkConfiguration getZkConf() {
        return zk.getZkConfiguration();
    }

    public InteractionProtocol getInteractionProtocol() {
        return protocol;
    }

    protected void deployIndexWithError() throws Exception {
        MasterContext context = masterContext;
        IndexDeployOperation deployOperation = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexPath(), 3);
        deployOperation.execute(context, EMPTY_LIST);
        deployOperation.nodeOperationsComplete(context, Collections.EMPTY_LIST);
    }

    protected void deployIndex(List<Node> nodes, List<NodeQueue> nodeQueues) throws Exception {
        MasterContext context = masterContext;
        IndexDeployOperation deployOperation = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexPath(), 3);
        deployOperation.execute(context, EMPTY_LIST);
        publishShards(nodes, nodeQueues);
        deployOperation.nodeOperationsComplete(context, Collections.EMPTY_LIST);
    }

    protected void publishShards(List<Node> nodes, List<NodeQueue> nodeQueues) throws InterruptedException {
        for (int i = 0; i < nodes.size(); i++) {
            publishShard(nodes.get(i), nodeQueues.get(i));
        }
    }

    protected void publishShard(Node node, NodeQueue nodeQueue) throws InterruptedException {
        InteractionProtocol protocol = getInteractionProtocol();
        Set<String> shardNames = ((ShardDeployOperation) nodeQueue.remove()).getShardNames();
        for (String shardName : shardNames) {
            protocol.publishShard(node, shardName);
        }
    }

}
