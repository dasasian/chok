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
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.util.ChokFileSystem;
import org.I0Itec.zkclient.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

public class BalanceIndexOperation extends AbstractIndexOperation {

    private static final long serialVersionUID = 1L;
    private final static Logger LOG = LoggerFactory.getLogger(AbstractIndexOperation.class);
    private final String indexName;

    public BalanceIndexOperation(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        IndexMetaData indexMD = protocol.getIndexMD(indexName);
        if (indexMD == null) {// could be undeployed in meantime
            LOG.info("skip balancing for index '" + indexName + "' cause it is already undeployed");
            return null;
        }
        if (!canAndShouldRegulateReplication(protocol, indexMD)) {
            LOG.info("skip balancing for index '" + indexName + "' cause there is no possible optimization");
            return null;
        }
        final URI indexUri = indexMD.getUri();
        try {
            ChokFileSystem fileSystem = context.getChokFileSystem(indexMD);
            if (!fileSystem.exists(indexUri)) {
                LOG.warn("skip balancing for index '" + indexName + "' cause source '" + indexUri + "' does not exists anymore");
                return null;
            }
        } catch (Exception e) {
            LOG.error("skip balancing of index '" + indexName + "' cause failed to access source '" + indexUri + "'", e);
            return null;
        }

        LOG.info("balancing shards for index '" + indexName + "'");
        try {
            return distributeIndexShards(context, indexMD, runningOperations);
        } catch (Exception e) {
            ExceptionUtil.rethrowInterruptedException(e);
            LOG.error("failed to deploy balance " + indexName, e);
            handleMasterDeployException(protocol, indexMD, e);
            return null;
        }
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        LOG.info("balancing of index " + indexName + " complete");
        IndexMetaData indexMD = context.getProtocol().getIndexMD(indexName);
        if (indexMD != null) {// could be undeployed in meantime
            handleDeploymentComplete(context, results, indexMD, false);
        }
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        for (MasterOperation operation : runningOperations) {
            if (operation instanceof BalanceIndexOperation && ((BalanceIndexOperation) operation).indexName.equals(indexName)) {
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
