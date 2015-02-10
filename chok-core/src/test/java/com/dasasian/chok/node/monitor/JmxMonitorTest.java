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
package com.dasasian.chok.node.monitor;

import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.testutil.AbstractTest;
import org.junit.Test;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class JmxMonitorTest extends AbstractTest {

    @Test
    public void testGetCpu() throws Exception {
        JmxMonitor monitor = new JmxMonitor();
        String nodeId = "someId";
        InteractionProtocol protocol = mock(InteractionProtocol.class);
        monitor.startMonitoring(nodeId, protocol);
        Thread.sleep(1200);
        verify(protocol, atLeastOnce()).setMetric(eq(nodeId), (MetricsRecord) anyObject());
    }
}
