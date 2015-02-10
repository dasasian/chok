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
package com.dasasian.chok.testutil.integration;

import com.dasasian.chok.client.DeployClient;
import com.dasasian.chok.client.IDeployClient;
import com.dasasian.chok.client.IIndexDeployFuture;
import com.dasasian.chok.master.Master;
import com.dasasian.chok.node.IContentServer;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.operation.master.IndexDeployOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.testutil.*;
import com.dasasian.chok.util.ChokException;
import com.dasasian.chok.util.NodeConfiguration;
import com.dasasian.chok.util.ZkConfiguration;
import com.google.common.collect.ImmutableList;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A container class for a whole chok cluster including:<br>
 * - zk server<br>
 * - master<br>
 * - nodes<br>
 */
public class ChokMiniCluster extends ExternalResource {

    protected static final Logger LOG = Logger.getLogger(ZkTestSystem.class);

    private final Class<? extends IContentServer> contentServerClass;
    private final int nodeStartPort;
    private final Class<? extends NodeConfigurationFactory> nodeConfigurationFactoryClass;
    private NodeConfigurationFactory nodeConfigurationFactory;

    private Master master;
    private Master secondaryMaster;
    private List<Node> nodes = new ArrayList<>();
    private InteractionProtocol protocol;
    private int initialNodeCount;
    private int startedNodes = 0;

    private TemporaryFolder temporaryFolder = null;
    private ZkTestSystem zkTestSystem = null;
    private ZkConfiguration zkConfiguration = null;

    public ChokMiniCluster(Class<? extends IContentServer> nodeServerClass, int nodeCount, int nodeStartPort, Class<? extends NodeConfigurationFactory> nodeConfigurationFactoryClass) {
        this.contentServerClass = nodeServerClass;
        this.initialNodeCount = nodeCount;
        this.nodeStartPort = nodeStartPort;
        this.nodeConfigurationFactoryClass = nodeConfigurationFactoryClass;
    }

    // executed before every test method
    @Override
    protected void before() throws Throwable {
        zkTestSystem = new ZkTestSystem();
        zkTestSystem.start();

        start(zkTestSystem, zkTestSystem.getZkConfiguration().getRootPath());
    }

    // executed after every test method
    @Override
    protected void after() {
        //cleanupZk();
        stop();

        zkTestSystem.stop();
        temporaryFolder.delete();
    }

    public void start(ZkTestSystem zkTestSystem, String zkRootPath) throws Exception {
        this.temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();

        this.nodeConfigurationFactory = nodeConfigurationFactoryClass.getConstructor(TemporaryFolder.class).newInstance(temporaryFolder);

        zkConfiguration = zkTestSystem.getZkConfiguration().rootPath(zkRootPath);

        LOG.info("~~~~~~~~~~~~~~~ starting chok mini cluster @ "+zkRootPath+" ~~~~~~~~~~~~~~~");
        final ZkClient zkClient = zkTestSystem.getZkClient();

        protocol = new InteractionProtocol(zkClient, zkConfiguration);

        master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, false);
        master.start();

        for (int i = 0; i < initialNodeCount; i++) {
            startAdditionalNode();
        }

        TestUtil.waitUntilLeaveSafeMode(master);
        TestUtil.waitUntilNumberOfLiveNode(protocol, nodes.size());
        TestUtil.waitUntilEmptyOperationQueues(protocol, master, nodes);
        LOG.info("~~~~~~~~~~~~~~~ chok mini cluster @ "+zkRootPath+" started ~~~~~~~~~~~~~~~");
    }

    public Node startAdditionalNode() throws Exception {
        // todo ? do I need to calculate the nodeStartPort + startedNodeCount
        NodeConfiguration nodeConfiguration = nodeConfigurationFactory.getConfiguration(nodeStartPort + startedNodes);
        Node node = new Node(protocol, nodeConfiguration, contentServerClass.newInstance());
        nodes.add(node);
        node.start();
        startedNodes++;
        return node;
    }

    public Master startSecondaryMaster() throws ChokException {
        secondaryMaster = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, false);
        secondaryMaster.start();
        return secondaryMaster;
    }

    public void restartMaster() throws Exception {
        master.shutdown();
        master = new Master(TestMasterConfiguration.getTestConfiguration(), protocol, false);
        master.start();
        TestUtil.waitUntilLeaveSafeMode(master);
    }

    public void stop(ZkTestSystem zkTestSystem, String zkRootPath) throws Exception {
        LOG.info("~~~~~~~~~~~~~~~ stopping chok mini cluster @ "+zkRootPath+" ~~~~~~~~~~~~~~~");
        stop();
        LOG.info("~~~~~~~~~~~~~~~ chok mini cluster @ " + zkRootPath + " stopped ~~~~~~~~~~~~~~~");

        ZkConfiguration conf = zkTestSystem.getZkConfiguration().rootPath(zkRootPath);
        zkTestSystem.cleanupZk(conf);
    }

    private void stop() {
        for (Node node : nodes) {
            if (node.isRunning()) {
                node.shutdown();
            }
        }
        try {
            Thread.sleep(100);
        }
        catch (InterruptedException e) {
            Thread.interrupted();
        }
        if (secondaryMaster != null) {
            secondaryMaster.shutdown();
        }
        master.shutdown();
    }

    public Node getNode(int i) {
        return nodes.get(i);
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public int getRunningNodeCount() {
        return nodes.size();
    }

    public int getStartedNodeCount() {
        return startedNodes;
    }

    public Node shutdownNode(int i) {
        Node node = nodes.remove(i);
        node.shutdown();
        return node;
    }

    public Node restartNode(int i) throws IOException {
        Node shutdownNode = getNode(i);
        NodeConfiguration nodeConfiguration = shutdownNode.getNodeConfiguration();
        shutdownNode(i);

        IContentServer contentServer = shutdownNode.getContext().getContentServer();
        Node node = new Node(protocol, nodeConfiguration, contentServer);
        node.start();
        nodes.add(i, node);
        return node;
    }

    public Node shutdownNodeRpc(int i) {
        Node node = nodes.get(i);
        node.getRpcServer().stop();
        return node;
    }

    public Master getMaster() {
        return master;
    }

    public List<String> deployTestIndexes(File indexFile, int replicationCount) throws InterruptedException {
        List<String> indices = new ArrayList<>();
        IDeployClient deployClient = new DeployClient(protocol);
        String indexName = indexFile.getName();
        IIndexDeployFuture deployFuture = deployClient.addIndex(indexName, indexFile.getAbsolutePath(), replicationCount);
        indices.add(indexName);
        deployFuture.joinDeployment();

        try {
            TestUtil.waitUntilIndexDeployed(protocol, indexName);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return indices;
    }

    public InteractionProtocol createInteractionProtocol() {
        return new InteractionProtocol(zkTestSystem.createZkClient(), zkConfiguration);
    }

    public InteractionProtocol getProtocol() {
        return protocol;
    }

    public ZkClient getZkClient() {
        return zkTestSystem.getZkClient();
    }

    public IndexMetaData deployIndex(TestIndex testIndex, int replication) throws Exception {
        return deployIndex(testIndex.getIndexName(), testIndex.getIndexFile(), replication);
    }

    public List<IndexMetaData> deployIndexes(TestIndex testIndex, int indexCount, int replication) throws Exception {
        ImmutableList.Builder<IndexMetaData> builder = ImmutableList.builder();
        for (int i = 0; i < indexCount; i++) {
            builder.add(deployIndex(testIndex.getIndexName() + i, testIndex.getIndexFile(), replication));
        }
        return builder.build();
    }

    public IndexMetaData deployIndex(String indexName, File indexFile, int replication) throws Exception {
        IndexDeployOperation deployOperation = new IndexDeployOperation(indexName, "file://" + indexFile.getAbsolutePath(), replication);
        InteractionProtocol protocol = getProtocol();
        protocol.addMasterOperation(deployOperation);
        TestUtil.waitUntilIndexDeployed(protocol, indexName);
        return protocol.getIndexMD(indexName);
    }

    public final int countShardDeployments(InteractionProtocol protocol, String indexName) {
        IndexMetaData indexMD = protocol.getIndexMD(indexName);
        int shardDeployCount = 0;
        for (IndexMetaData.Shard shard : indexMD.getShards()) {
            shardDeployCount += protocol.getShardNodes(shard.getName()).size();
        }
        return shardDeployCount;
    }

    public ZkConfiguration getZkConfiguration() {
        if(zkConfiguration != null) {
            return zkConfiguration;
        }
        return zkTestSystem.getZkConfiguration();
    }
}
