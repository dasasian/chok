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

import com.dasasian.chok.node.monitor.IMonitor;
import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.operation.node.ShardRedeployOperation;
import com.dasasian.chok.protocol.ConnectedComponent;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.protocol.metadata.NodeMetaData;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.NodeConfiguration;
import com.dasasian.chok.util.ThrottledInputStream;
import com.dasasian.chok.util.ThrottledInputStream.ThrottleSemaphore;
import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.NetworkUtil;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

public class Node implements ConnectedComponent {

    protected final static Logger LOG = LoggerFactory.getLogger(Node.class);
    private final NodeConfiguration nodeConfiguration;
    private final IContentServer contentServer;
    private final ChokFileSystem.Factory chokFileSystemFactory;
    protected final InteractionProtocol protocol;
    protected NodeContext context;
    protected String nodeName;
    private Server rpcServer;
    private IMonitor monitor;
    private Thread nodeOperatorThread;
    private boolean stopped;

    public Node(InteractionProtocol protocol, final NodeConfiguration configuration, IContentServer contentServer, ChokFileSystem.Factory chokFileSystemFactory) {
        this.protocol = protocol;
        this.contentServer = contentServer;
        this.nodeConfiguration = configuration;
        this.chokFileSystemFactory = chokFileSystemFactory;
    }

    /*
     * Starting the hadoop RPC server that response to query requests. We iterate
     * over a port range of node.server.port.start + 10000
     */
    private static Server startRPCServer(String hostName, final int startPort, IContentServer nodeManaged, int handlerCount) {
        int serverPort = startPort;
        int tryCount = 10000;
        Server rpcServer = null;
        while (rpcServer == null) {
            try {
                rpcServer = new RPC.Builder(new Configuration()).setProtocol(nodeManaged.getClass()).setInstance(nodeManaged).setPort(serverPort).setNumHandlers(handlerCount).setVerbose(false).build();
                LOG.info(nodeManaged.getClass().getSimpleName() + " server started on : " + hostName + ":" + serverPort);
            } catch (final BindException e) {
                if (serverPort - startPort < tryCount) {
                    serverPort++;
                    // try again
                } else {
                    throw new RuntimeException("tried " + tryCount + " ports and no one is free...");
                }
            } catch (final IOException e) {
                throw new RuntimeException("unable to create rpc server", e);
            }
        }
        rpcServer.start();
        return rpcServer;
    }

    /**
     * Boots the node
     */
    public void start() {
        if (stopped) {
            throw new IllegalStateException("Node cannot be started again after it was shutdown.");
        }
        LOG.info("starting rpc server with  server class = " + contentServer.getClass().getCanonicalName());
        String hostName = NetworkUtil.getLocalhostName();
        rpcServer = startRPCServer(hostName, nodeConfiguration.getStartPort(), contentServer, nodeConfiguration.getRpcHandlerCount());
        nodeName = hostName + ":" + rpcServer.getListenerAddress().getPort();
        contentServer.init(nodeName, nodeConfiguration);

        // we add hostName and port to the shardFolder to allow multiple nodes per
        // server with the same configuration
        // todo is there a better way of doing this?
        Path shardsFolder = nodeConfiguration.getShardFolder().resolve(nodeName.replaceAll(":", "_"));
        LOG.info("local shard folder: " + shardsFolder);
        int throttleInKbPerSec = nodeConfiguration.getShardDeployThrottle();
        ThrottleSemaphore throttleSemaphore = null;
        if (throttleInKbPerSec > 0) {
            LOG.info("throttling of shard deployment to " + throttleInKbPerSec + " kilo-bytes per second");
            throttleSemaphore = new ThrottleSemaphore(throttleInKbPerSec * 1024);
        }
        final ShardManager shardManager  = new ShardManager(shardsFolder, chokFileSystemFactory, throttleSemaphore);
        context = new NodeContext(protocol, this, shardManager, contentServer);
        protocol.registerComponent(this);

        startMonitor(nodeName, nodeConfiguration);
        init();
        LOG.info("started node '" + nodeName + "'");
    }

    private void init() {
        redeployInstalledShards();
        NodeMetaData nodeMetaData = new NodeMetaData(nodeName);
        NodeQueue nodeOperationQueue = protocol.publishNode(this, nodeMetaData);
        startOperatorThread(nodeOperationQueue);
    }

    private void startOperatorThread(NodeQueue nodeOperationQueue) {
        nodeOperatorThread = new Thread(new NodeOperationProcessor(nodeOperationQueue, context));
        nodeOperatorThread.setName(NodeOperationProcessor.class.getSimpleName() + ": " + getName());
        nodeOperatorThread.setDaemon(true);
        nodeOperatorThread.start();
    }

    @Override
    public synchronized void reconnect() {
        LOG.info(nodeName + " reconnected");
        init();
    }

    @Override
    public synchronized void disconnect() {
        LOG.info(nodeName + " disconnected");
        try {
            do {
                LOG.info("trying to stop node-processor...");
                nodeOperatorThread.interrupt();
                nodeOperatorThread.join(2500);
            } while (nodeOperatorThread.isAlive());
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        // we keep serving the shards
    }

    private void redeployInstalledShards() {
        List<String> installedShards = context.getShardManager().getInstalledShards();
        ShardRedeployOperation redeployOperation = new ShardRedeployOperation(installedShards);
        try {
            redeployOperation.execute(context);
        } catch (InterruptedException e) {
            ExceptionUtil.convertToRuntimeException(e);
        }
    }

    private void startMonitor(String nodeName, NodeConfiguration conf) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("starting node monitor");
        }
        try {
            Class<? extends IMonitor> monitorClass = conf.getMonitorClass();
            monitor = monitorClass.newInstance();
            monitor.startMonitoring(nodeName, protocol);
        } catch (Exception e) {
            LOG.error("Unable to start node monitor:", e);
        }
    }

    public void shutdown() {
        if (stopped) {
            throw new IllegalStateException("already stopped");
        }
        LOG.info("shutdown " + nodeName + " ...");
        stopped = true;

        if (monitor != null) {
            monitor.stopMonitoring();
        }
        nodeOperatorThread.interrupt();
        try {
            nodeOperatorThread.join();
        } catch (InterruptedException e) {
            Thread.interrupted();// proceed
        }

        protocol.unregisterComponent(this);
        rpcServer.stop();
        try {
            context.getContentServer().shutdown();
        } catch (Throwable t) {
            LOG.error("Error shutting down server", t);
        }
        LOG.info("shutdown " + nodeName + " finished");
    }

    public String getName() {
        return nodeName;
    }

    public NodeContext getContext() {
        return context;
    }

    public int getRPCServerPort() {
        return rpcServer.getListenerAddress().getPort();
    }

    public boolean isRunning() {
        // TODO jz: improve this whole start/stop/isRunning thing
        return context != null && !stopped;
    }

    public void join() throws InterruptedException {
        rpcServer.join();
    }

    public Server getRpcServer() {
        return rpcServer;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        shutdown();
    }

    @Override
    public String toString() {
        return nodeName;
    }

    public InteractionProtocol getProtocol() {
        return protocol;
    }

    public NodeConfiguration getNodeConfiguration() {
        return nodeConfiguration;
    }

    protected static class NodeOperationProcessor implements Runnable {

        private final NodeQueue _queue;
        private final NodeContext nodeContext;

        public NodeOperationProcessor(NodeQueue queue, NodeContext nodeContext) {
            _queue = queue;
            this.nodeContext = nodeContext;
        }

        @Override
        public void run() {
            try {
                while (nodeContext.getNode().isRunning()) {
                    try {
                        NodeOperation operation = _queue.peek();
                        OperationResult operationResult = null;
                        try {
                            LOG.info("executing " + operation);
                            operationResult = operation.execute(nodeContext);
                        } catch (Exception e) {
                            LOG.error(nodeContext.getNode().getName() + ": failed to execute " + operation, e);
                            operationResult = new OperationResult(nodeContext.getNode().getName(), e);
                            ExceptionUtil.rethrowInterruptedException(e);
                        } finally {
                            _queue.complete(operationResult);// only remove after finish
                        }
                    } catch (Throwable e) {
                        ExceptionUtil.rethrowInterruptedException(e);
                        LOG.error(nodeContext.getNode().getName() + ": operation failure ", e);
                    }
                }
            } catch (InterruptedException | ZkInterruptedException e) {
                Thread.interrupted();
            }
            LOG.info("node operation processor for " + nodeContext.getNode().getName() + " stopped");
        }
    }
}
