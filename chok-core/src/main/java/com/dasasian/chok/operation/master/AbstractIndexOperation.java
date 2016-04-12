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

import com.dasasian.chok.master.CopyToAllNodesDistributionPolicy;
import com.dasasian.chok.master.IDeployPolicy;
import com.dasasian.chok.master.MasterContext;
import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.node.DeployResult;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.operation.node.ShardDeployOperation;
import com.dasasian.chok.operation.node.ShardUndeployOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.ReplicationReport;
import com.dasasian.chok.protocol.metadata.IndexDeployError;
import com.dasasian.chok.protocol.metadata.IndexDeployError.ErrorType;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public abstract class AbstractIndexOperation implements MasterOperation {

    public static final char INDEX_SHARD_NAME_SEPARATOR = '#';
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractIndexOperation.class);
    private ImmutableSetMultimap<String, String> newShard2Nodes = ImmutableSetMultimap.of();

    public static String createShardName(String indexName, String shardPath) {
        int lastIndexOf = shardPath.lastIndexOf("/");
        if (lastIndexOf == -1) {
            lastIndexOf = 0;
        }
        String shardFolderName = shardPath.substring(lastIndexOf + 1, shardPath.length());
        if (shardFolderName.endsWith(".zip")) {
            shardFolderName = shardFolderName.substring(0, shardFolderName.length() - 4);
        }
        return indexName + INDEX_SHARD_NAME_SEPARATOR + shardFolderName;
    }

    public static String getIndexNameFromShardName(String shardName) {
        try {
            return shardName.substring(0, shardName.indexOf(INDEX_SHARD_NAME_SEPARATOR));
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(shardName + " is not a valid shard name");
        }
    }

    protected List<OperationId> distributeIndexShards(MasterContext context, final IndexMetaData indexMD, List<MasterOperation> runningOperations) throws IndexDeployException {
        InteractionProtocol protocol = context.getProtocol();
        Collection<String> liveNodes = protocol.getLiveNodes();
        if (liveNodes.isEmpty()) {
            throw new IndexDeployException(ErrorType.NO_NODES_AVAILIBLE, "no nodes availible");
        }

        Set<Shard> shards = indexMD.getShards();
        Set<String> shardNames = shards.stream().map(Shard::getName).collect(Collectors.toSet());

        IDeployPolicy deployPolicy = (indexMD.isCopyToAllNodes()) ? new CopyToAllNodesDistributionPolicy() : context.getDeployPolicy();

        // now distribute shards
        final ImmutableSetMultimap<String, String> currentShard2NodesMap = getCurrentShard2NodesMap(protocol, shards, runningOperations);

        final ImmutableSetMultimap<String, String> newNode2ShardMap = deployPolicy
                .createDistributionPlan(shardNames, liveNodes, currentShard2NodesMap, indexMD.getReplicationLevel());

        return getOperationIds(indexMD, protocol, currentShard2NodesMap, newNode2ShardMap);
    }

    private List<OperationId> getOperationIds(IndexMetaData indexMD, InteractionProtocol protocol, ImmutableSetMultimap<String, String> currentShard2NodesMap, ImmutableSetMultimap<String, String> newNode2ShardMap) {
        final ImmutableSetMultimap<String, String> currentNode2ShardsMap = currentShard2NodesMap.inverse();
        // System.out.println(distributionMap);// node to shards
        Set<String> operationNodes = newNode2ShardMap.keySet();

        List<OperationId> operationIds = new ArrayList<>(operationNodes.size());

        ImmutableSetMultimap.Builder<String, String> newShard2NodesBuilder = ImmutableSetMultimap.builder();

        for (String node : operationNodes) {
            Collection<String> nodeShards = newNode2ShardMap.get(node);
            ImmutableSet<String> currentShards = currentNode2ShardsMap.get(node);

            List<String> listOfAdded = nodeShards.stream().filter(shard -> !currentShards.contains(shard)).collect(Collectors.toList());
            if (!listOfAdded.isEmpty()) {
                ShardDeployOperation deployInstruction = new ShardDeployOperation(indexMD.getAutoReload());
                listOfAdded.stream().forEach(shard -> {
                    deployInstruction.addShard(shard, indexMD.getShardUri(shard));
                    newShard2NodesBuilder.put(shard, node);
                });
                OperationId operationId = protocol.addNodeOperation(node, deployInstruction);
                operationIds.add(operationId);
            }

            List<String> listOfRemoved = currentShards.stream().filter(shard -> !nodeShards.contains(shard)).collect(Collectors.toList());
            if (!listOfRemoved.isEmpty()) {
                ShardUndeployOperation undeployInstruction = new ShardUndeployOperation(listOfRemoved);
                OperationId operationId = protocol.addNodeOperation(node, undeployInstruction);
                operationIds.add(operationId);
            }
        }
        this.newShard2Nodes = newShard2NodesBuilder.build();
        return operationIds;
    }

    private ImmutableSetMultimap<String, String> getCurrentShard2NodesMap(InteractionProtocol protocol, Set<Shard> shards, List<MasterOperation> runningOperations) {
        ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();
        builder.putAll(protocol.getShard2NodesMap(Shard.getShardNames(shards)));
        runningOperations.stream()
                .filter(masterOperation -> masterOperation instanceof AbstractIndexOperation)
                .map(masterOperation -> ((AbstractIndexOperation) masterOperation))
                .map(AbstractIndexOperation::getNewShard2Nodes)
                .forEach(builder::putAll);
        return builder.build();
    }

    protected ImmutableSetMultimap<String, String> getNewShard2Nodes() {
        return newShard2Nodes;
    }

    protected boolean canAndShouldRegulateReplication(InteractionProtocol protocol, IndexMetaData indexMD) {
        int liveNodes = protocol.getLiveNodeCount();
        ReplicationReport replicationReport = protocol.getReplicationReport(indexMD, liveNodes);
        return canAndShouldRegulateReplication(replicationReport, liveNodes);
    }

    protected boolean canAndShouldRegulateReplication(ReplicationReport replicationReport, int liveNodes) {
        if (replicationReport.isBalanced()) {
            return false;
        }
        if (replicationReport.isUnderreplicated() && liveNodes <= replicationReport.getMinimalShardReplicationCount()) {
            return false;
        }
        return true;
    }

    protected void handleMasterDeployException(InteractionProtocol protocol, IndexMetaData indexMD, Exception e) {
        ErrorType errorType;
        if (e instanceof IndexDeployException) {
            errorType = ((IndexDeployException) e).getErrorType();
        } else {
            errorType = ErrorType.UNKNOWN;
        }
        IndexDeployError deployError = new IndexDeployError(indexMD.getName(), errorType);
        deployError.setException(e);
        indexMD.setDeployError(deployError);
        protocol.updateIndexMD(indexMD);
    }

    protected void handleDeploymentComplete(MasterContext context, List<OperationResult> results, IndexMetaData indexMD, boolean newIndex) {
        InteractionProtocol protocol = context.getProtocol();
        ReplicationReport replicationReport = protocol.getReplicationReport(indexMD, protocol.getLiveNodeCount());
        if (replicationReport.isDeployed()) {
            indexMD.setDeployError(null);
            updateShardMetaData(results, indexMD);
            // we ignore possible shard errors
            if (canAndShouldRegulateReplication(replicationReport, protocol.getLiveNodeCount())) {
                protocol.addMasterOperation(new BalanceIndexOperation(indexMD.getName()));
            }
        } else {
            IndexDeployError deployError = new IndexDeployError(indexMD.getName(), ErrorType.SHARDS_NOT_DEPLOYABLE);
            // node-crashed produces null
            results.stream().filter(operationResult -> operationResult != null).forEach(operationResult -> {// node-crashed produces null
                DeployResult deployResult = (DeployResult) operationResult;
                for (Entry<String, Exception> entry : deployResult.getShardExceptions().entrySet()) {
                    deployError.addShardError(entry.getKey(), entry.getValue());
                }
            });
            indexMD.setDeployError(deployError);
        }
        if (newIndex) {
            protocol.publishIndex(indexMD);
        } else {
            protocol.updateIndexMD(indexMD);
        }
    }

    private void updateShardMetaData(List<OperationResult> results, IndexMetaData indexMD) {
        // node-crashed produces null
        results.stream().filter(Objects::nonNull).forEach(operationResult -> {// node-crashed produces null
            DeployResult deployResult = (DeployResult) operationResult;
            for (Entry<String, Map<String, String>> entry : deployResult.getShardMetaDataMaps().entrySet()) {
                Map<String, String> existingMap = indexMD.getShard(entry.getKey()).getMetaDataMap();
                Map<String, String> newMap = entry.getValue();
                if (existingMap.size() > 0 && !existingMap.equals(newMap)) {
                    // maps from different nodes but for the same shard should have
                    // the same content
                    LOG.warn("new shard metadata differs from existing one. old: " + existingMap + " new: " + newMap);
                }
                existingMap.putAll(newMap);
            }
        });
    }

}
