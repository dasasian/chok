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
package com.dasasian.chok.master;

import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.master.MasterOperation;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.testutil.mockito.SerializableCountDownLatchAnswer;

import java.io.Serializable;
import java.util.List;

/**
 * Created by damith.chandrasekara on 1/25/15.
 */
public class TestMasterOperation implements MasterOperation, Serializable {
    private final SerializableCountDownLatchAnswer answer;

    public TestMasterOperation(SerializableCountDownLatchAnswer answer) {

        this.answer = answer;
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        try {
            answer.answer(null);
        } catch (Throwable throwable) {
            throw new Exception(throwable);
        }
        return null;
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
    }
}
