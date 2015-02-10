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
import com.dasasian.chok.protocol.metadata.IndexDeployError;
import com.dasasian.chok.protocol.metadata.IndexDeployError.ErrorType;
import com.dasasian.chok.protocol.metadata.IndexMetaData;

import java.util.List;

public class CheckIndicesOperation extends AbstractIndexOperation {

    private static final long serialVersionUID = 1L;

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        List<String> liveNodes = protocol.getLiveNodes();
        List<String> indices = protocol.getIndices();

        for (String indexName : indices) {
            IndexMetaData indexMD = protocol.getIndexMD(indexName);
            if (indexMD != null) { // can be removed already
                if ((indexMD.hasDeployError() && isRecoverable(indexMD.getDeployError(), liveNodes.size())) || canAndShouldRegulateReplication(protocol, indexMD)) {
                    protocol.addMasterOperation(new BalanceIndexOperation(indexName));
                }
            }
        }
        return null;
    }

    private boolean isRecoverable(IndexDeployError deployError, int nodeCount) {
        if (deployError.getErrorType() == ErrorType.NO_NODES_AVAILIBLE && nodeCount > 0) {
            return true;
        }
        return false;
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        // nothing to do
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        for (MasterOperation operation : runningOperations) {
            if (operation instanceof CheckIndicesOperation) {
                return ExecutionInstruction.ADD_TO_QUEUE_TAIL;
            }
        }
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode());
    }
}
