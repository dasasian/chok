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

import com.dasasian.chok.master.MasterContext;
import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.operation.node.ShardUndeployOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.dasasian.chok.util.ZkConfiguration;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import com.google.common.collect.ImmutableSetMultimap;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;
import java.util.stream.Collectors;

public class IndexUndeployOperation implements MasterOperation {

    private static final long serialVersionUID = 1L;
    private final String indexName;
    private IndexMetaData indexMD;

    public IndexUndeployOperation(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        indexMD = protocol.getIndexMD(indexName);

        ImmutableSetMultimap<String, String> shard2NodesMap = protocol.getShard2NodesMap(Shard.getShardNames(indexMD.getShards()));

        ImmutableSetMultimap<String, String> node2ShardsMap = shard2NodesMap.inverse();

        List<OperationId> nodeOperationIds = node2ShardsMap.asMap().entrySet().stream().map(e -> protocol.addNodeOperation(e.getKey(), new ShardUndeployOperation(e.getValue())))
                .collect(Collectors.toList());

        protocol.unpublishIndex(indexName);
        return nodeOperationIds;
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        ZkClient zkClient = context.getProtocol().getZkClient();
        ZkConfiguration zkConf = context.getProtocol().getZkConfiguration();
        for (Shard shard : indexMD.getShards()) {
            zkClient.deleteRecursive(zkConf.getPath(PathDef.SHARD_TO_NODES, shard.getName()));
        }
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        for (MasterOperation operation : runningOperations) {
            if (operation instanceof IndexUndeployOperation && ((IndexUndeployOperation) operation).indexName.equals(indexName)) {
                return ExecutionInstruction.CANCEL;
            }
        }
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + indexName;
    }

}
