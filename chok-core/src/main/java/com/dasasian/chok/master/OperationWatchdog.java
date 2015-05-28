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
import com.dasasian.chok.protocol.ConnectedComponent;
import com.dasasian.chok.protocol.IAddRemoveListener;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * When watchdog for a list of {@link com.dasasian.chok.operation.node.NodeOperation}s. The watchdog is finished
 * if all operations are done or the nodes of the incomplete nodes went down.
 */
public class OperationWatchdog implements ConnectedComponent, Serializable {

    protected final static Logger LOG = LoggerFactory.getLogger(OperationWatchdog.class);
    private static final long serialVersionUID = 1L;
    private final String queueElementId;
    private final List<OperationId> openOperationIds;
    private final List<OperationId> operationIds;
    private final MasterOperation masterOperation;
    private MasterContext context;

    public OperationWatchdog(String queueElementId, MasterOperation masterOperation, List<OperationId> operationIds) {
        this.queueElementId = queueElementId;
        this.operationIds = operationIds;
        this.masterOperation = masterOperation;
        openOperationIds = new ArrayList<>(operationIds);
    }

    public void start(MasterContext context) {
        this.context = context;
        subscribeNotifications();
    }

    private synchronized void subscribeNotifications() {
        checkDeploymentForCompletion();
        if (isDone()) {
            return;
        }

        InteractionProtocol protocol = context.getProtocol();
        protocol.registerChildListener(this, PathDef.NODES_LIVE, new IAddRemoveListener() {
            @Override
            public void removed(String name) {
                checkDeploymentForCompletion();
            }

            @Override
            public void added(String name) {
                // nothing todo
            }
        });
        IZkDataListener dataListener = new IZkDataListener() {
            @Override
            public void handleDataDeleted(String arg0) throws Exception {
                checkDeploymentForCompletion();
            }

            @Override
            public void handleDataChange(String arg0, Object arg1) throws Exception {
                // nothing todo
            }
        };
        for (OperationId operationId : openOperationIds) {
            protocol.registerNodeOperationListener(this, operationId, dataListener);
        }
        checkDeploymentForCompletion();
    }

    protected final synchronized void checkDeploymentForCompletion() {
        if (isDone()) {
            return;
        }

        List<String> liveNodes = context.getProtocol().getLiveNodes();
        for (Iterator<OperationId> iter = openOperationIds.iterator(); iter.hasNext(); ) {
            OperationId operationId = iter.next();
            if (!context.getProtocol().isNodeOperationQueued(operationId) || !liveNodes.contains(operationId.getNodeName())) {
                iter.remove();
            }
        }
        if (isDone()) {
            finishWatchdog();
        } else {
            LOG.info("still " + getOpenOperationCount() + " open deploy operations");
        }
    }

    public synchronized void cancel() {
        context.getProtocol().unregisterComponent(this);
        this.notifyAll();
    }

    private synchronized void finishWatchdog() {
        InteractionProtocol protocol = context.getProtocol();
        protocol.unregisterComponent(this);
        try {
            List<OperationResult> operationResults = new ArrayList<>(openOperationIds.size());
            for (OperationId operationId : operationIds) {
                OperationResult operationResult = protocol.getNodeOperationResult(operationId, true);
                if (operationResult != null && operationResult.getUnhandledError() != null) {
                    // TODO jz: do we need to inform the master operation ?
                    LOG.error("received unhandled error from node " + operationId.getNodeName() + ":" + operationResult.getUnhandledError());
                }
                operationResults.add(operationResult);// we add null ones
            }
            masterOperation.nodeOperationsComplete(context, operationResults);
        } catch (Exception e) {
            LOG.info("operation complete action of " + masterOperation + " failed", e);
        }
        LOG.info("watch for " + masterOperation + " finished");
        this.notifyAll();
        context.getMasterQueue().removeWatchdog(this);
    }

    public String getQueueElementId() {
        return queueElementId;
    }

    public MasterOperation getOperation() {
        return masterOperation;
    }

    public List<OperationId> getOperationIds() {
        return operationIds;
    }

    public final int getOpenOperationCount() {
        return openOperationIds.size();
    }

    public boolean isDone() {
        return openOperationIds.isEmpty();
    }

    public final synchronized void join() throws InterruptedException {
        join(0);
    }

    public final synchronized void join(long timeout) throws InterruptedException {
        if (!isDone()) {
            this.wait(timeout);
        }
    }

    @Override
    public final void disconnect() {
        // handled by master
    }

    @Override
    public final void reconnect() {
        // handled by master
    }

}
