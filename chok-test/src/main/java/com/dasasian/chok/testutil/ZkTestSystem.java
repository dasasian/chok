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

import com.dasasian.chok.protocol.ChokZkSerializer;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.ZkChokUtil;
import com.dasasian.chok.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.slf4j.Logger;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ZkTestSystem extends ExternalResource {

    public static final String ZK_ROOT_PATH = "/zk_testsystem";
    protected static final Logger LOG = LoggerFactory.getLogger(ZkTestSystem.class);
    private final static int PORT = 10001;
    private TemporaryFolder temporaryFolder;
    private ZkServer zkServer;
    private ZkConfiguration conf;

    // executed before every test method
    @Override
    protected void before() throws IOException {
        start();
    }

    public void start() throws IOException {
        this.temporaryFolder = new TemporaryFolder();
        this.temporaryFolder.create();
        //cleanupZk();
        LOG.info("~~~~~~~~~~~~~~~ starting zk system ~~~~~~~~~~~~~~~");
        try {
            conf = TestZkConfiguration.getTestConfiguration(temporaryFolder.newFolder(), PORT, ZK_ROOT_PATH);
            zkServer = ZkChokUtil.startZkServer(conf);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        LOG.info("~~~~~~~~~~~~~~~ zk system started ~~~~~~~~~~~~~~~");
    }

    // executed after every test method
    @Override
    protected void after() {
        stop();
    }

    public void stop() {
        cleanupZk();
        zkServer.shutdown();

        this.temporaryFolder.delete();
    }

    public void cleanupZk() {
        cleanupZk(conf);
    }

    public void cleanupZk(ZkConfiguration zkConfiguration) {
        String zkRootPath = zkConfiguration.getRootPath();
        try {
            LOG.info("cleanup zk namespace:" + zkRootPath);
            getZkClient().deleteRecursive(zkRootPath);
        } catch (Exception e) {
//            LOG.warn("Error while cleaning up ZooKeeper", e);
        }

        try {
            LOG.info("unsubscribing " + getZkClient().numberOfListeners() + " listeners");
            getZkClient().unsubscribeAll();
        } catch (Exception e) {
//            LOG.warn("Error while unsubscribing listeners from ZooKeeper", e);
        }
    }

    public ZkClient getZkClient() {
        return createZkClient();
//        return zkServer.getZkClient();
    }

//    public InteractionProtocol createInteractionProtocol() {
//        return new InteractionProtocol(zkServer.getZkClient(), conf);
//    }

    public InteractionProtocol createInteractionProtocol() {
        return new InteractionProtocol(createZkClient(), conf);
//        return new InteractionProtocol(createZkClient(), conf);
    }

    public ZkConfiguration getZkConfiguration() {
        return conf;
    }

    public int getServerPort() {
        return PORT;
    }

    public ZkClient createZkClient() {
        final ZkClient zkClient = ZkChokUtil.startZkClient("localhost:" + PORT);
        zkClient.setZkSerializer(new ChokZkSerializer());
        return zkClient;
    }

//    public void showStructure() {
//        getInteractionProtocol().showStructure(true);
//    }

    public ZkServer getZkServer() {
        return zkServer;
    }
}
