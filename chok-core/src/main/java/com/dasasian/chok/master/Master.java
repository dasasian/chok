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

import com.dasasian.chok.operation.master.CheckIndicesOperation;
import com.dasasian.chok.operation.master.RemoveObsoleteShardsOperation;
import com.dasasian.chok.protocol.ConnectedComponent;
import com.dasasian.chok.protocol.IAddRemoveListener;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.protocol.metadata.Version;
import com.dasasian.chok.util.ChokException;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.MasterConfiguration;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import com.google.common.base.Preconditions;
import org.I0Itec.zkclient.NetworkUtil;
import org.I0Itec.zkclient.ZkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class Master implements ConnectedComponent {

    protected final static Logger LOG = LoggerFactory.getLogger(Master.class);

    protected volatile OperatorThread operatorThread;
    private final MasterConfiguration masterConfiguration;
    protected InteractionProtocol protocol;
    private final ChokFileSystem.Factory chokFileSystemFactory;
    private final String masterName;
    private ZkServer zkServer;
    private final boolean shutdownClient;
    private final IDeployPolicy deployPolicy;
    private final long safeModeMaxTime;

    public Master(MasterConfiguration masterConfiguration, InteractionProtocol protocol, ChokFileSystem.Factory chokFileSystemFactory, ZkServer zkServer, boolean shutdownClient) throws ChokException {
        this(masterConfiguration, protocol, chokFileSystemFactory, shutdownClient);
        this.zkServer = zkServer;
    }

    public Master(MasterConfiguration masterConfiguration, InteractionProtocol protocol, ChokFileSystem.Factory chokFileSystemFactory, boolean shutdownClient) throws ChokException {
        this.masterConfiguration = masterConfiguration;
        this.protocol = protocol;
        this.chokFileSystemFactory = chokFileSystemFactory;
        this.masterName = NetworkUtil.getLocalhostName() + "_" + UUID.randomUUID().toString();
        this.shutdownClient = shutdownClient;
        protocol.registerComponent(this);
        deployPolicy = masterConfiguration.getDeployPolicy();
        safeModeMaxTime = masterConfiguration.getSafeModeMaxTime();
    }

    public synchronized void start() {
        Preconditions.checkState(!isShutdown(), "master was already started");
        becomePrimaryOrSecondaryMaster();
    }

    @Override
    public synchronized void reconnect() {
        disconnect();// just to be sure we do not open a 2nd operator thread
        becomePrimaryOrSecondaryMaster();
    }

    @Override
    public synchronized void disconnect() {
        if (isMaster()) {
            operatorThread.interrupt();
            try {
                operatorThread.join();
            } catch (InterruptedException e) {
                Thread.interrupted();
                // proceed
            }
            operatorThread = null;
        }
    }

    private synchronized void becomePrimaryOrSecondaryMaster() {
        if (isShutdown()) {
            return;
        }
        MasterQueue queue = protocol.publishMaster(this);
        if (queue != null) {
            protocol.setVersion(Version.readFromJar());
            LOG.info(getMasterName() + " became master with " + queue.size() + " waiting master operations");
            startNodeManagement();
            MasterContext masterContext = new MasterContext(protocol, this, deployPolicy, queue, chokFileSystemFactory);
            operatorThread = new OperatorThread(masterContext, safeModeMaxTime);
            operatorThread.start();
        }
    }

    public synchronized boolean isInSafeMode() {
        return !isMaster() || operatorThread.isInSafeMode();
    }

    public Collection<String> getConnectedNodes() {
        return protocol.getLiveNodes();
    }

    public synchronized MasterContext getContext() {
        if (!isMaster()) {
            return null;
        }
        return operatorThread.getContext();
    }

    public synchronized boolean isMaster() {
        return operatorThread != null;
    }

    private synchronized boolean isShutdown() {
        return protocol == null;
    }

    public String getMasterName() {
        return masterName;
    }

    public void handleMasterDisappearedEvent() {
        becomePrimaryOrSecondaryMaster();
    }

    private void startNodeManagement() {
        LOG.info("start managing nodes...");
        List<String> nodes = protocol.registerChildListener(this, PathDef.NODES_LIVE, new IAddRemoveListener() {
            @Override
            public void removed(String name) {
                synchronized (Master.this) {
                    if (!isInSafeMode() && protocol.isIndexAutoRepairEnabled()) {
                        protocol.addMasterOperation(new CheckIndicesOperation());
                    }
                }
            }

            @Override
            public void added(String name) {
                synchronized (Master.this) {
                    if (!isMaster()) {
                        return;
                    }
                    protocol.addMasterOperation(new RemoveObsoleteShardsOperation(name));
                    if (!isInSafeMode() && protocol.isIndexAutoRepairEnabled()) {
                        protocol.addMasterOperation(new CheckIndicesOperation());
                    }
                }
            }
        });
        protocol.addMasterOperation(new CheckIndicesOperation());
        for (String node : nodes) {
            protocol.addMasterOperation(new RemoveObsoleteShardsOperation(node));
        }
        LOG.info("found following nodes connected: " + nodes);
    }

    public synchronized void shutdown() {
        if (protocol != null) {
            protocol.unregisterComponent(this);
            if (isMaster()) {
                operatorThread.interrupt();
                try {
                    operatorThread.join();
                    operatorThread = null;
                } catch (final InterruptedException e1) {
                    // proceed
                }
            }
            if (shutdownClient) {
                protocol.disconnect();
            }
            protocol = null;
            if (zkServer != null) {
                zkServer.shutdown();
            }
        }
    }

}
