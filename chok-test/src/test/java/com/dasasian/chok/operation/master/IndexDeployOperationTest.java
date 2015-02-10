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

import com.dasasian.chok.node.Node;
import com.dasasian.chok.operation.node.DeployResult;
import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.operation.node.ShardDeployOperation;
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.protocol.metadata.IndexDeployError;
import com.dasasian.chok.protocol.metadata.IndexDeployError.ErrorType;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.dasasian.chok.testutil.Mocks;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class IndexDeployOperationTest extends AbstractMasterNodeZkTest {

    @Test
    public void testDeployError_NoNodes() throws Exception {
        IndexDeployOperation deployCommand = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexPath(), 3);
        deployCommand.execute(masterContext, EMPTY_LIST);
        checkDeployError(ErrorType.NO_NODES_AVAILIBLE, testIndex.getShardCount());
    }

    @Test
    public void testDeployError_IndexNotAccessable() throws Exception {
        IndexDeployOperation deployCommand = new IndexDeployOperation(testIndex.getIndexName(), "wrongIndexPath", 3);
        deployCommand.execute(masterContext, EMPTY_LIST);
        checkDeployError(ErrorType.INDEX_NOT_ACCESSIBLE, 0);
    }

    private void checkDeployError(ErrorType errorType, int shardCount) throws Exception {
        // check results
        assertEquals(1, getInteractionProtocol().getIndices().size());
        IndexMetaData indexMD = getInteractionProtocol().getIndexMD(testIndex.getIndexName());
        assertNotNull(indexMD);
        assertTrue(indexMD.hasDeployError());
        IndexDeployError error = indexMD.getDeployError();
        assertNotNull(error);
        assertEquals(errorType, error.getErrorType());
        Set<Shard> shards = indexMD.getShards();
        assertEquals(shardCount, shards.size());
        for (Shard shard : shards) {
            assertTrue(getInteractionProtocol().getShardNodes(shard.getName()).isEmpty());
        }
    }

    @Test
    public void testDeployError_ShardsNotDeployable() throws Exception {
        // add nodes
        List<Node> nodes = Mocks.mockNodes(3);
        Mocks.publishNodes(getInteractionProtocol(), nodes);

        // add index
        int replicationLevel = 3;
        IndexDeployOperation operation = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexPath(), replicationLevel);
        operation.execute(masterContext, EMPTY_LIST);

        // now complete the deployment
        operation.nodeOperationsComplete(masterContext, Collections.EMPTY_LIST);
        checkDeployError(ErrorType.SHARDS_NOT_DEPLOYABLE, testIndex.getShardCount());
    }

    @Test
    public void testDeployErrorExceptions_ShardsNotDeployable() throws Exception {
        // add nodes
        List<Node> nodes = Mocks.mockNodes(3);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);

        // add index
        int replicationLevel = 3;
        IndexDeployOperation operation = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexPath(), replicationLevel);
        operation.execute(masterContext, EMPTY_LIST);

        // now complete the deployment
        List<OperationResult> results = new ArrayList<>();
        for (NodeQueue nodeQueue : nodeQueues) {
            NodeOperation nodeOperation = nodeQueue.peek();
            DeployResult deployResult = new DeployResult(testIndex.getIndexName());
            Set<String> nodeShards = ((ShardDeployOperation) nodeOperation).getShardNames();
            for (String shardName : nodeShards) {
                deployResult.addShardException(shardName, new Exception());
            }
            results.add(deployResult);
        }

        operation.nodeOperationsComplete(masterContext, results);
        checkDeployError(ErrorType.SHARDS_NOT_DEPLOYABLE, testIndex.getShardCount());

        IndexMetaData indexMD = getInteractionProtocol().getIndexMD(testIndex.getIndexName());
        IndexDeployError error = indexMD.getDeployError();
        Set<Shard> shards = indexMD.getShards();
        for (Shard shard : shards) {
            assertEquals(3, error.getShardErrors(shard.getName()).size());
        }
    }

    @Test
    public void testDeploy() throws Exception {
        // add nodes
        List<Node> nodes = Mocks.mockNodes(3);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);

        // add index
        int replicationLevel = 3;
        IndexDeployOperation operation = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexPath(), replicationLevel);
        operation.execute(masterContext, EMPTY_LIST);

        // check results
        Set<String> shards = new HashSet<>();
        int shardOnNodeCount = 0;
        for (NodeQueue nodeQueue : nodeQueues) {
            assertEquals(1, nodeQueue.size());
            NodeOperation nodeOperation = nodeQueue.peek();
            assertNotNull(nodeOperation);
            assertTrue(nodeOperation instanceof ShardDeployOperation);

            Set<String> nodeShards = ((ShardDeployOperation) nodeOperation).getShardNames();
            assertEquals(testIndex.getShardCount(), nodeShards.size());
            shards.addAll(nodeShards);
            shardOnNodeCount += nodeShards.size();
        }
        assertEquals(testIndex.getShardCount() * replicationLevel, shardOnNodeCount);
        assertEquals(testIndex.getShardCount(), shards.size());

        // now complete the deployment
        publishShards(nodes, nodeQueues);
        operation.nodeOperationsComplete(masterContext, Collections.EMPTY_LIST);
        assertEquals(1, getInteractionProtocol().getIndices().size());
        IndexMetaData indexMD = getInteractionProtocol().getIndexMD(testIndex.getIndexName());
        assertNotNull(indexMD);
        assertNull(indexMD.getDeployError());
    }

    @Test
    public void testDeployRespectsCurrentRunningDeployments() throws Exception {
        // add nodes
        List<Node> nodes = Mocks.mockNodes(2 * testIndex.getShardCount());
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);

        // add index
        int replicationLevel = 1;
        IndexDeployOperation operation1 = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexPath(), replicationLevel);
        operation1.execute(masterContext, EMPTY_LIST);
        List<MasterOperation> runningOps = new ArrayList<>();
        runningOps.add(operation1);
        IndexDeployOperation operation2 = new IndexDeployOperation(testIndex.getIndexName() + "2", testIndex.getIndexPath(), replicationLevel);
        operation2.execute(masterContext, runningOps);

        // check results
        List<Integer> nodeQueueSizes = new ArrayList<>();
        for (NodeQueue nodeQueue : nodeQueues) {
            nodeQueueSizes.add(nodeQueue.size());
        }
        for (Integer integer : nodeQueueSizes) {
            assertEquals("unequal shard distribution: " + nodeQueueSizes, 1, integer.intValue());
        }
    }

    @Test
    public void testDeployShardMd() throws Exception {
        List<Node> nodes = Mocks.mockNodes(3);
        List<NodeQueue> queues = Mocks.publishNodes(getInteractionProtocol(), nodes);

        IndexDeployOperation operation = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexPath(), 3);
        operation.execute(masterContext, EMPTY_LIST);
        publishShards(nodes, queues);

        ArrayList<OperationResult> results = new ArrayList<>();
        DeployResult deployResult1 = new DeployResult(nodes.get(0).getName());
        DeployResult deployResult2 = new DeployResult(nodes.get(1).getName());
        DeployResult deployResult3 = new DeployResult(nodes.get(2).getName());
        Map<String, String> metaMap = new HashMap<>();
        metaMap.put("a", "1");
        String shard1Name = AbstractIndexOperation.createShardName(testIndex.getIndexName(), testIndex.getIndexFile().listFiles()[0].getAbsolutePath());
        deployResult1.addShardMetaDataMap(shard1Name, metaMap);
        deployResult2.addShardMetaDataMap(shard1Name, metaMap);
        deployResult3.addShardMetaDataMap(shard1Name, metaMap);
        results.add(deployResult1);
        results.add(deployResult2);
        results.add(deployResult3);

        operation.nodeOperationsComplete(masterContext, results);
        IndexMetaData indexMD = getInteractionProtocol().getIndexMD(testIndex.getIndexName());
        assertEquals(1, indexMD.getShard(shard1Name).getMetaDataMap().size());
        assertEquals(metaMap, indexMD.getShard(shard1Name).getMetaDataMap());
    }

    @Test
    public void testDeployShardMdWithMissingNodeResult() throws Exception {
        List<Node> nodes = Mocks.mockNodes(3);
        Mocks.publishNodes(getInteractionProtocol(), nodes);
        List<NodeQueue> queues = Mocks.publishNodes(getInteractionProtocol(), nodes);

        IndexDeployOperation operation = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexPath(), 3);
        operation.execute(masterContext, EMPTY_LIST);
        publishShards(nodes, queues);

        ArrayList<OperationResult> results = new ArrayList<>();
        DeployResult deployResult1 = new DeployResult(nodes.get(0).getName());
        DeployResult deployResult2 = null;
        DeployResult deployResult3 = new DeployResult(nodes.get(2).getName());
        Map<String, String> metaMap = new HashMap<>();
        metaMap.put("a", "1");
        String shard1Name = AbstractIndexOperation.createShardName(testIndex.getIndexName(), testIndex.getIndexFile().listFiles()[0].getAbsolutePath());
        deployResult1.addShardMetaDataMap(shard1Name, metaMap);
        deployResult3.addShardMetaDataMap(shard1Name, metaMap);
        results.add(deployResult1);
        results.add(deployResult2);
        results.add(deployResult3);

        operation.nodeOperationsComplete(masterContext, results);
        IndexMetaData indexMD = getInteractionProtocol().getIndexMD(testIndex.getIndexName());
        assertEquals(1, indexMD.getShard(shard1Name).getMetaDataMap().size());
        assertEquals(metaMap, indexMD.getShard(shard1Name).getMetaDataMap());
    }

    @Test
    public void testDeployUnderreplicatedIndex() throws Exception {
        // add nodes
        List<Node> nodes = Mocks.mockNodes(3);
        List<NodeQueue> nodeQueues = Mocks.publishNodes(getInteractionProtocol(), nodes);

        // add index
        int replicationLevel = 3;
        IndexDeployOperation deployOperation = new IndexDeployOperation(testIndex.getIndexName(), testIndex.getIndexPath(), replicationLevel);
        deployOperation.execute(masterContext, EMPTY_LIST);

        // publis only for one node
        publishShard(nodes.get(0), nodeQueues.get(0));

        deployOperation.nodeOperationsComplete(masterContext, Collections.EMPTY_LIST);
        assertEquals(1, getInteractionProtocol().getIndices().size());
        IndexMetaData indexMD = getInteractionProtocol().getIndexMD(testIndex.getIndexName());
        assertNotNull(indexMD);
        assertNull(indexMD.getDeployError());

        // balance index should have been be triggered
        MasterQueue masterQueue = getInteractionProtocol().publishMaster(mockMaster);
        MasterOperation operation = masterQueue.peek();
        assertNotNull(operation);
        assertTrue(operation instanceof BalanceIndexOperation);
    }

}
