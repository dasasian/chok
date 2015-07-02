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
package com.dasasian.chok.integration;

import com.dasasian.chok.client.DeployClient;
import com.dasasian.chok.client.IIndexDeployFuture;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.TestUtil;
import com.dasasian.chok.testutil.ZkTestSystem;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.testutil.server.sleep.SleepClient;
import com.dasasian.chok.testutil.server.sleep.SleepServer;
import com.dasasian.chok.util.ChokException;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.UtilModule;
import com.dasasian.chok.util.ZkConfiguration;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the situation where you using 2 instances of Chok to talk
 * to 2 pools of nodes at the same time.
 */
public class MultiClusterTest extends AbstractTest {

    public static final String INDEX1 = "pool1";
    public static final String INDEX2 = "pool2";
    public static final String ZK_ROOT_PATH1 = "/MultiClusterTest/pool1";
    public static final String ZK_ROOT_PATH2 = "/MultiClusterTest/pool2";
    protected static final Logger LOG = LoggerFactory.getLogger(MultiClusterTest.class);
    private static final int POOL_SIZE_1 = 9;
    private static final int POOL_SIZE_2 = 7;

    private static final int NUM_SHARDS_1 = 140;
    private static final int NUM_SHARDS_2 = 70;

    private static ZkTestSystem zk;

    private static SleepClient client1;
    private static SleepClient client2;

    private static ChokMiniCluster cluster1;
    private static ChokMiniCluster cluster2;

    protected static Injector injector = Guice.createInjector(new UtilModule());

    @BeforeClass
    public static void setUp() throws Exception {
        zk = new ZkTestSystem();
        zk.start();

        ZkConfiguration conf1 = zk.getZkConfiguration().rootPath(ZK_ROOT_PATH1);
        ZkConfiguration conf2 = zk.getZkConfiguration().rootPath(ZK_ROOT_PATH2);

        // start cluster 1
        cluster1 = new ChokMiniCluster(SleepServer.class, POOL_SIZE_1, 30000, TestNodeConfigurationFactory.class, injector.getInstance(ChokFileSystem.Factory.class));
        cluster1.start(zk, ZK_ROOT_PATH1);

        // start cluster 1
        cluster2 = new ChokMiniCluster(SleepServer.class, POOL_SIZE_2, 40000, TestNodeConfigurationFactory.class, injector.getInstance(ChokFileSystem.Factory.class));
        cluster2.start(zk, ZK_ROOT_PATH2);

        // Create lots of empty shards. SleepServer does not use the directory, but
        // Node does.
        LOG.info("Creating indicies");
        File index1 = temporaryFolder.newFolder();
        setupIndex(index1, NUM_SHARDS_1);
        File index2 = temporaryFolder.newFolder();
        setupIndex(index2, NUM_SHARDS_2);

        // Deploy shards to pool1.
        LOG.info("Deploying index 1");
        deployIndex(cluster1.getProtocol(), INDEX1, index1);

        // Deploy shards to pool2.
        LOG.info("Deploying index 2");
        deployIndex(cluster2.getProtocol(), INDEX2, index2);

        // Verify setup.
        // LOG.info("\n\nPOOL 1 STRUCTURE:\n");
        // cluster1.getProtocol().showStructure(false);
        // LOG.info("\n\nPOOL 2 STRUCTURE:\n");
        // cluster2.getProtocol().showStructure(false);

        // Back end ready to run. Create clients.
        LOG.info("Creating clients");
        client1 = new SleepClient(conf1);
        client2 = new SleepClient(conf2);

        // sleep so that the client(s) can update
        TestUtil.waitUntilClientHasIndex(client1.getClient(), INDEX1);
        TestUtil.waitUntilClientHasIndex(client2.getClient(), INDEX2);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        client1.close();
        client2.close();
        cluster1.stop(zk, ZK_ROOT_PATH1);
        cluster2.stop(zk, ZK_ROOT_PATH2);
        zk.cleanupZk();
        zk.stop();
    }

    private static void deployIndex(InteractionProtocol protocol, String indexName, File index) throws InterruptedException {
        DeployClient deployClient1 = new DeployClient(protocol);
        IIndexDeployFuture deployment = deployClient1.addIndex(indexName, index.toURI(), 1);
        LOG.info("Joining deployment on " + deployment.getClass().getName());
        deployment.joinDeployment();
    }

    private static void setupIndex(File index, int size) {
        for (int i = 0; i < size; i++) {
            File f = new File(index, "shard" + i);
            if (!f.mkdirs()) {
                throw new RuntimeException("unable to create folder: " + f.getAbsolutePath());
            }
        }
    }

    @Test
    public void testSerial() throws ChokException {
        assertEquals(NUM_SHARDS_1, client1.sleep(0L));
        assertEquals(NUM_SHARDS_2, client2.sleep(0L));
        //
        Random rand = new Random("Multi chok".hashCode());
        for (int i = 0; i < 200; i++) {
            if (rand.nextBoolean()) {
                assertEquals(NUM_SHARDS_1, client1.sleep(rand.nextInt(5), rand.nextInt(5)));
            }
            if (rand.nextBoolean()) {
                assertEquals(NUM_SHARDS_2, client2.sleep(rand.nextInt(5), rand.nextInt(5)));
            }
        }
    }

    @Test
    public void testParallel() throws InterruptedException, ChokException {
        assertEquals(NUM_SHARDS_1, client1.sleep(0L));
        assertEquals(NUM_SHARDS_2, client2.sleep(0L));

        LOG.info("Testing multithreaded access to multiple Chok instances...");
        Long start = System.currentTimeMillis();
        Random rand = new Random("Multi chok2".hashCode());
        List<Thread> threads = Lists.newArrayList();
        final List<Throwable> throwables = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < 15; i++) {
            final Random rand2 = new Random(rand.nextInt());
            Thread thread = new Thread(() -> {
                try {
                    for (int j = 0; j < 400; j++) {
                        if (rand2.nextBoolean()) {
                            assertEquals(NUM_SHARDS_1, client1.sleep(rand2.nextInt(2), rand2.nextInt(2)));
                        }
                        if (rand2.nextBoolean()) {
                            assertEquals(NUM_SHARDS_2, client2.sleep(rand2.nextInt(2), rand2.nextInt(2)));
                        }
                    }
                } catch (Throwable t) {
                    LOG.error("Error! ", t);
                    throwables.add(t);
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        LOG.info("Took " + (System.currentTimeMillis() - start) + " msec");
        for (Throwable t : throwables) {
            LOG.error("Exception thrown:", t);
            t.printStackTrace();
        }
        assertTrue(throwables.isEmpty());
    }

}
