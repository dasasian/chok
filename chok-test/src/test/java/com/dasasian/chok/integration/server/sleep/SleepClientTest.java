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
package com.dasasian.chok.integration.server.sleep;

import com.dasasian.chok.client.Client;
import com.dasasian.chok.client.DeployClient;
import com.dasasian.chok.client.IDeployClient;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.TestUtil;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.testutil.server.sleep.SleepClient;
import com.dasasian.chok.testutil.server.sleep.SleepServer;
import com.dasasian.chok.util.ChokException;
import com.dasasian.chok.util.ClassUtil;
import com.google.common.collect.Lists;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Test for {@link SleepClient}.
 */
public class SleepClientTest extends AbstractTest {

    public static final String INDEX_NAME = "index1";
    protected static final Logger LOG = LoggerFactory.getLogger(SleepClientTest.class);
    private static final String[] INDEXES_ARRAY = new String[]{INDEX_NAME};
    @Rule
    public ChokMiniCluster miniCluster = new ChokMiniCluster(SleepServer.class, 1, 20000, TestNodeConfigurationFactory.class);
    private SleepClient client;

    @Before
    public void start() throws Exception {
//        protocol = protocol;
        // Create lots of empty shards. SleepServer does not use the directory, but
        // Node does.
        LOG.info("Creating indicies");
        File index1 = temporaryFolder.newFolder();
        setupIndex(index1, 1);

        // Deploy shards to pool1.
        LOG.info("Deploying index 1");


        IDeployClient deployClient = new DeployClient(miniCluster.getProtocol());
        deployClient.addIndex(INDEX_NAME, index1.getAbsolutePath(), 1).joinDeployment();
        client = new SleepClient(miniCluster.getZkConfiguration());
        // sleep so that the client can update
        TestUtil.waitUntilClientHasIndex(client.getClient(), INDEX_NAME);
    }

    private void setupIndex(File index, int size) {
        for (int i = 0; i < size; i++) {
            File f = new File(index, "shard" + i);
            if (!f.mkdirs()) {
                throw new RuntimeException("unable to create folder: " + f.getAbsolutePath());
            }
        }
    }


    @After
    public void stop() throws IOException {
        client.close();
    }

    @Test
    public void testDelay() throws ChokException {
        long start = System.currentTimeMillis();
        client.sleepIndices(0, INDEXES_ARRAY);
        long d1 = System.currentTimeMillis() - start;
        System.out.println("time 1 = " + d1);
        start = System.currentTimeMillis();
        client.sleepIndices(1000, INDEXES_ARRAY);
        long d2 = System.currentTimeMillis() - start;
        System.out.println("time 2 = " + d2);
        assertTrue(d2 - d1 > 200);
    }

    @Test
    public void testMultiThreadedAccess() throws Exception {
        Random rand = new Random("sleepy".hashCode());
        List<Thread> threads = Lists.newArrayList();
        final List<Exception> exceptions = Lists.newArrayList();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            final Random rand2 = new Random(rand.nextInt());
            Thread t = new Thread(new Runnable() {
                public void run() {
                    for (int j = 0; j < 50; j++) {
                        int n = rand2.nextInt(20);
                        try {
                            client.sleepIndices(n, INDEXES_ARRAY);
                        } catch (Exception e) {
                            System.err.println(e);
                            exceptions.add(e);
                            break;
                        }
                    }
                }
            });
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        System.out.println("Took " + (System.currentTimeMillis() - startTime) + " msec.");
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
    }

    @Test
    public void testNonExistantShard() throws Exception {
        try {
            client.sleepShards(0, 0, new String[]{"doesNotExist"});
            fail("Should have failed.");
        } catch (ChokException e) {
            assertEquals("Shard 'doesNotExist' is currently not reachable", e.getMessage());
        }
    }

    @Test
    public void testNonExistantIndex() throws Exception {
        try {

            client.sleepIndices(0, 0, new String[]{"doesNotExist"});
            fail("Should have failed.");
        } catch (ChokException e) {
            assertTrue(e.getMessage().startsWith("Index [pattern(s)] '[doesNotExist]' do not match to any deployed index: ["));
        }
    }

    @Test
    public void testCleanupOfClientWatchesOnUndeploy_CHOK_182() throws Exception {
        IDeployClient deployClient = new DeployClient(miniCluster.getProtocol());
        IndexMetaData indexMD = deployClient.getIndexMetaData(INDEX_NAME);
        deployClient.removeIndex(INDEX_NAME);
        TestUtil.waitUntilShardsUndeployed(miniCluster.getProtocol(), indexMD);

        Client client = this.client.getClient();
        Assertions.assertThat(client.getIndices()).excludes((Object) INDEXES_ARRAY);
        Map<String, Set<Watcher>> existWatches = getExistsWatches(client);
        Assertions.assertThat(existWatches).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Set<Watcher>> getExistsWatches(Client client) {
        InteractionProtocol protocol = (InteractionProtocol) ClassUtil.getPrivateFieldValue(client, "protocol");
        ZkClient zkClient = protocol.getZkClient();
        ZkConnection zkConnection = (ZkConnection) ClassUtil.getPrivateFieldValue(zkClient, "_connection");
        ZooKeeper zookeeper = zkConnection.getZookeeper();
        Object watchManager = ClassUtil.getPrivateFieldValue(zookeeper, "watchManager");
        return (Map<String, Set<Watcher>>) ClassUtil.getPrivateFieldValue(watchManager, "existWatches");
    }
}
