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
package com.dasasian.chok.node;

import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.testutil.mockito.SerializableCountDownLatchAnswer;

import java.io.Serializable;

/**
 * Created by damith.chandrasekara on 1/25/15.
 */
public class TestNodeOperation implements NodeOperation, Serializable {

    private final SerializableCountDownLatchAnswer answer;

    public TestNodeOperation(SerializableCountDownLatchAnswer answer) {
        this.answer = answer;
    }

    @Override
    public OperationResult execute(NodeContext context) throws InterruptedException {
        try {
            answer.answer(null);
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
        return null;
    }
}
