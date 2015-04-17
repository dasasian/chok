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
import org.I0Itec.zkclient.ExceptionUtil;

import java.util.List;

@SuppressWarnings("serial")
public class IndexReinitializeOperation extends IndexDeployOperation {

    public IndexReinitializeOperation(IndexMetaData indexMD) {
        super(indexMD.getName(), indexMD.getPath(), indexMD.getReplicationLevel());
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        try {
            indexMetaData.getShards().addAll(readShardsFromFs(indexMetaData.getName(), indexMetaData.getPath()));
            protocol.updateIndexMD(indexMetaData);
        } catch (Exception e) {
            ExceptionUtil.rethrowInterruptedException(e);
            handleMasterDeployException(protocol, indexMetaData, e);
        }
        return null;
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        // nothing todo
    }

}
