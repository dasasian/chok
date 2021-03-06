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
package com.dasasian.chok.testutil.mockito;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class SleepingAnswer implements Answer<Void> {

    private final long _timeToSleep;

    public SleepingAnswer() {
        this(Long.MAX_VALUE);
    }

    public SleepingAnswer(long timeToSleep) {
        _timeToSleep = timeToSleep;
    }

    @Override
    public Void answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(_timeToSleep);
        return null;
    }

}
