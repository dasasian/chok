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

import java.io.Serializable;
import java.util.List;

/**
 * An operation carried out by the master node.
 * <p>
 * If an {@link InterruptedException} is thrown during the operations
 * {@link #execute(MasterContext, java.util.List)} method (which can happen during master change
 * or zookeeper reconnect) the operation can either catch and handle or rethrow
 * it. Rethrowing it will lead to complete reexecution of the operation.
 */
public interface MasterOperation extends Serializable {

    /**
     * Called before {@link #execute(MasterContext, List)} to evaluate if this
     * operation is blocked, delayed, etc by another running
     * {@link MasterOperation}.
     *
     * @param runningOperations the running operations
     * @return instruction
     * @throws java.lang.Exception when an error occurs
     */
    ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception;

    /**
     * @param runningOperations currently running {@link MasterOperation}s
     * @return null or a list of operationId which have to be completed before
     * {@link #nodeOperationsComplete(MasterContext, java.util.List)} method is called.
     * @param context the context
     * @throws java.lang.Exception when an error occurs
     */
    List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception;

    /**
     * Called when all operations are complete or the nodes of the incomplete
     * operations went down. This method is NOT called if
     * {@link #execute(MasterContext, java.util.List)} returns null or an empty list of
     * {@link OperationId}s.
     * @param context the context
     * @param results the operation results
     * @throws java.lang.Exception when an error occurs
     */
    void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception;

    enum ExecutionInstruction {
        EXECUTE, CANCEL, ADD_TO_QUEUE_TAIL
    }

}
