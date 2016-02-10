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
package com.dasasian.chok.client;

import com.dasasian.chok.protocol.InteractionProtocol;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for Client
 * Created by damith.chandrasekara on 8/27/15.
 */
public class ClientTest {
    @Test
    public void testClose() throws Exception {
        InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
        INodeProxyManager proxyManager = Mockito.mock(INodeProxyManager.class);
        Client client = new Client(new DefaultNodeSelectionPolicy(), protocol, proxyManager, 3);

        client.close();
        Mockito.verify(protocol).disconnect();
    }

}
