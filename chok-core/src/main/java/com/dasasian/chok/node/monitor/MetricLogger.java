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

import com.dasasian.chok.protocol.ConnectedComponent;
import com.dasasian.chok.protocol.IAddRemoveListener;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class MetricLogger implements IZkDataListener, ConnectedComponent {

    private final static Logger LOG = LoggerFactory.getLogger(MetricLogger.class);
    protected final InteractionProtocol protocol;
    private final OutputType outputType;
    private final ReentrantLock lock;
    private long loggedRecords = 0;
    public MetricLogger(OutputType outputType, InteractionProtocol protocol) {
        this.protocol = protocol;
        this.outputType = outputType;
        this.protocol.registerComponent(this);
        List<String> children = this.protocol.registerChildListener(this, PathDef.NODE_METRICS, new IAddRemoveListener() {
            @Override
            public void removed(String name) {
                unsubscribeDataUpdates(name);
            }

            @Override
            public void added(String name) {
                MetricsRecord metric = MetricLogger.this.protocol.getMetric(name);
                logMetric(metric);
                subscribeDataUpdates(name);
            }
        });
        children.forEach(this::subscribeDataUpdates);
        lock = new ReentrantLock();
        lock.lock();
    }

    protected void subscribeDataUpdates(String nodeName) {
        protocol.registerDataListener(this, PathDef.NODE_METRICS, nodeName, this);
    }

    protected void unsubscribeDataUpdates(String nodeName) {
        protocol.unregisterDataChanges(this, PathDef.NODE_METRICS, nodeName);
    }

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
        MetricsRecord metrics = (MetricsRecord) data;
        logMetric(metrics);
    }

    protected void logMetric(MetricsRecord metrics) {
        switch (outputType) {
            case Log4J:
                LOG.info(metrics.toString());
                break;
            case SysOut:
                System.out.println(metrics.toString());
                break;
            default:
                throw new IllegalStateException("output type " + outputType + " not supported");
        }
        loggedRecords++;
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
        // nothing todo
    }

    public void join() throws InterruptedException {
        synchronized (lock) {
            lock.wait();
        }
    }

    public long getLoggedRecords() {
        return loggedRecords;
    }

    @Override
    public void disconnect() {
        lock.unlock();
    }

    @Override
    public void reconnect() {
        // nothing to do
    }

    public enum OutputType {
        Log4J, SysOut
    }

}
