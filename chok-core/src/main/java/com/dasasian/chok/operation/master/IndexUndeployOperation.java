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
import com.dasasian.chok.util.CollectionUtil;
import com.dasasian.chok.util.ZkConfiguration;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.ZkClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexUndeployOperation implements MasterOperation {

    private static final long serialVersionUID = 1L;
    private final String _indexName;
    private IndexMetaData _indexMD;

    public IndexUndeployOperation(String indexName) {
        _indexName = indexName;
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        _indexMD = protocol.getIndexMD(_indexName);

        Map<String, List<String>> shard2NodesMap = protocol.getShard2NodesMap(Shard.getShardNames(_indexMD.getShards()));
        Map<String, List<String>> node2ShardsMap = CollectionUtil.invertListMap(shard2NodesMap);
        Set<String> nodes = node2ShardsMap.keySet();
        List<OperationId> nodeOperationIds = new ArrayList<>(nodes.size());
        for (String node : nodes) {
            List<String> nodeShards = node2ShardsMap.get(node);
            OperationId operationId = protocol.addNodeOperation(node, new ShardUndeployOperation(nodeShards));
            nodeOperationIds.add(operationId);
        }
        protocol.unpublishIndex(_indexName);
        return nodeOperationIds;
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        ZkClient zkClient = context.getProtocol().getZkClient();
        ZkConfiguration zkConf = context.getProtocol().getZkConfiguration();
        for (Shard shard : _indexMD.getShards()) {
            zkClient.deleteRecursive(zkConf.getPath(PathDef.SHARD_TO_NODES, shard.getName()));
        }
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        for (MasterOperation operation : runningOperations) {
            if (operation instanceof IndexUndeployOperation && ((IndexUndeployOperation) operation)._indexName.equals(_indexName)) {
                return ExecutionInstruction.CANCEL;
            }
        }
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + _indexName;
    }

}
