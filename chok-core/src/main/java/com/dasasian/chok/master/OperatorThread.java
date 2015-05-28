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
import com.dasasian.chok.operation.master.MasterOperation.ExecutionInstruction;
import com.dasasian.chok.protocol.MasterQueue;
import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Responsible for executing all {@link MasterOperation}s which are offered to
 * the master queue sequentially.
 */
class OperatorThread extends Thread {

    protected final static Logger LOG = LoggerFactory.getLogger(OperatorThread.class);

    private final MasterContext masterContext;
    private final MasterQueue masterQueue;
    private final OperationRegistry operationRegistry;
    private final long safeModeMaxTime;

    private boolean safeMode = true;

    public OperatorThread(final MasterContext context, final long safeModeMaxTime) {
        masterContext = context;
        masterQueue = context.getMasterQueue();
        operationRegistry = new OperationRegistry(context);
        setDaemon(true);
        setName(getClass().getSimpleName());
        this.safeModeMaxTime = safeModeMaxTime;
    }

    public boolean isInSafeMode() {
        return safeMode;
    }

    public OperationRegistry getOperationRegistry() {
        return operationRegistry;
    }

    public MasterContext getContext() {
        return masterContext;
    }

    @Override
    public void run() {
        try {
            LOG.info("starting...");
            runInSafeMode();
            recreateWatchdogs();

            while (true) {
                try {
                    // TODO jz: poll only for a certain amount of time and then execute a
                    // global check operation ?
                    MasterOperation operation = masterQueue.peek();
                    List<OperationId> nodeOperationIds = null;
                    try {
                        List<MasterOperation> runningOperations = operationRegistry.getRunningOperations();
                        ExecutionInstruction instruction = operation.getExecutionInstruction(runningOperations);
                        nodeOperationIds = executeOperation(operation, instruction, runningOperations);
                    } catch (Exception e) {
                        ExceptionUtil.rethrowInterruptedException(e);
                        LOG.error("failed to execute " + operation, e);
                    }
                    if (nodeOperationIds != null && !nodeOperationIds.isEmpty()) {
                        OperationWatchdog watchdog = masterQueue.moveOperationToWatching(operation, nodeOperationIds);
                        operationRegistry.watchFor(watchdog);
                    } else {
                        masterQueue.remove();
                    }
                } catch (Throwable e) {
                    ExceptionUtil.rethrowInterruptedException(e);
                    LOG.error("master operation failure", e);
                }
            }
        } catch (final InterruptedException | ZkInterruptedException e) {
            Thread.interrupted();
            // let go the thread
        }
        operationRegistry.shutdown();
        LOG.info("operator thread stopped");
    }

    private void recreateWatchdogs() {
        List<OperationWatchdog> watchdogs = masterContext.getMasterQueue().getWatchdogs();
        for (OperationWatchdog watchdog : watchdogs) {
            if (watchdog.isDone()) {
                LOG.info("release done watchdog " + watchdog);
                masterQueue.removeWatchdog(watchdog);
            } else {
                operationRegistry.watchFor(watchdog);
            }
        }
    }

    private List<OperationId> executeOperation(MasterOperation operation, ExecutionInstruction instruction, List<MasterOperation> runningOperations) throws Exception {
        List<OperationId> operationIds = null;
        switch (instruction) {
            case EXECUTE:
                LOG.info("executing operation '" + operation + "'");
                operationIds = operation.execute(masterContext, runningOperations);
                break;
            case CANCEL:
                // just do nothing
                LOG.info("skipping operation '" + operation + "'");
                break;
            case ADD_TO_QUEUE_TAIL:
                LOG.info("adding operation '" + operation + "' to end of queue");
                masterQueue.add(operation);
                break;
            default:
                throw new IllegalStateException("execution instruction " + instruction + " not handled");
        }
        return operationIds;
    }

    private void runInSafeMode() throws InterruptedException {
        safeMode = true;
        // List<String> knownNodes = protocol.getKnownNodes(); //TODO jz: use known
        // nodes ?
        List<String> previousLiveNodes = masterContext.getProtocol().getLiveNodes();
        long lastChange = System.currentTimeMillis();
        try {
            while (previousLiveNodes.isEmpty() || lastChange + safeModeMaxTime > System.currentTimeMillis()) {
                LOG.trace("SAFE MODE: No nodes available or state unstable within the last " + safeModeMaxTime + " ms.");
                Thread.sleep(safeModeMaxTime / 4);// TODO jz: listen on life nodes ?

                List<String> currentLiveNodes = masterContext.getProtocol().getLiveNodes();
                if (currentLiveNodes.size() != previousLiveNodes.size()) {
                    lastChange = System.currentTimeMillis();
                    previousLiveNodes = currentLiveNodes;
                }
            }
            LOG.info("SAFE MODE: leaving safe mode with " + previousLiveNodes.size() + " connected nodes");
        } finally {
            safeMode = false;
        }
    }

}
