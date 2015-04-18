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
package com.dasasian.chok.testutil;

import com.dasasian.chok.protocol.InteractionProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

public class AbstractZkTest extends AbstractTest {

    @ClassRule
    public static ZkTestSystem zk = new ZkTestSystem();

    public InteractionProtocol protocol;

    @Before
    public void setInteractionProtocol() {
        protocol = zk.createInteractionProtocol();
    }

    @After
    public void cleanupZk() {
        protocol.disconnect();
        zk.cleanupZk();
    }
}