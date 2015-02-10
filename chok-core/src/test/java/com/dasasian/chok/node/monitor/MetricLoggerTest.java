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

import com.dasasian.chok.node.monitor.MetricLogger.OutputType;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.testutil.AbstractZkTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MetricLoggerTest extends AbstractZkTest {

    @Test
    public void testLogMetric() throws Exception {
        InteractionProtocol protocol = zk.createInteractionProtocol();
        MetricLogger metricLogger = new MetricLogger(OutputType.Log4J, protocol);
        protocol.setMetric("node1", new MetricsRecord("node1"));
        Thread.sleep(500);
        protocol.setMetric("node1", new MetricsRecord("node1"));
        Thread.sleep(500);
        assertEquals(2, metricLogger.getLoggedRecords());
        protocol.unregisterComponent(metricLogger);
        protocol.disconnect();
    }
}
