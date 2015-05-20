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
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RemoveObsoleteShardsOperation implements MasterOperation {

    private final static Logger LOG = Logger.getLogger(AbstractIndexOperation.class);

    private static final long serialVersionUID = 1L;
    private final String nodeName;

    public RemoveObsoleteShardsOperation(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getNodeName() {
        return nodeName;
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        Collection<String> nodeShards = protocol.getNodeShards(nodeName);
        List<String> obsoleteShards = collectObsoleteShards(protocol, nodeShards, runningOperations);
        if (!obsoleteShards.isEmpty()) {
            LOG.info("found following shards obsolete on node " + nodeName + ": " + obsoleteShards);
            protocol.addNodeOperation(nodeName, new ShardUndeployOperation(obsoleteShards));
        }

        return null;
    }

    private List<String> collectObsoleteShards(InteractionProtocol protocol, Collection<String> nodeShards, List<MasterOperation> runningOperations) {
        List<String> obsoleteShards = new ArrayList<>();
        for (String shardName : nodeShards) {
            try {
                String indexName = AbstractIndexOperation.getIndexNameFromShardName(shardName);
                IndexMetaData indexMD = protocol.getIndexMD(indexName);
                if (indexMD == null && !containsDeployOperation(runningOperations, indexName)) {
                    // index has been removed
                    obsoleteShards.add(shardName);
                }
            } catch (IllegalArgumentException e) {
                LOG.warn("found shard with invalid name '" + shardName + "' - instruct removal");
                obsoleteShards.add(shardName);
            }
        }
        return obsoleteShards;
    }

    private boolean containsDeployOperation(List<MasterOperation> runningOperations, String indexName) {
        for (MasterOperation masterOperation : runningOperations) {
            if (masterOperation instanceof IndexDeployOperation && ((IndexDeployOperation) masterOperation).getIndexName().equals(indexName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        // nothing todo
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + nodeName;
    }

}
