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
package com.dasasian.chok.lucene.integration;

import com.dasasian.chok.client.DeployClient;
import com.dasasian.chok.client.IIndexDeployFuture;
import com.dasasian.chok.lucene.LuceneServer;
import com.dasasian.chok.lucene.testutil.LuceneSearchExecutor;
import com.dasasian.chok.lucene.testutil.LuceneTestResources;
import com.dasasian.chok.lucene.testutil.TestLuceneNodeConfigurationFactory;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.testutil.AbstractZkTest;
import com.dasasian.chok.testutil.TestIoUtil;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.TestUtil;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.testutil.loadtest.LoadTestMasterOperation;
import com.dasasian.chok.testutil.server.sleep.SleepServer;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.UtilModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LuceneLoadTest extends AbstractZkTest {

    protected static Injector injector = Guice.createInjector(new UtilModule());

    public static final String LUCENE_ZK_ROOT_PATH = "/LuceneLoadIntegrationTest/luceneCluster";
    public static final String LOAD_TEST_ZK_ROOT_PATH = "/LuceneLoadIntegrationTest/loadTestCluster";
    protected static final Logger LOG = LoggerFactory.getLogger(LuceneLoadTest.class);
    private static final int NODE_COUNT_LOAD_TEST = 3;
    private static final int NODE_COUNT_LUCENE = 5;
    private ChokMiniCluster luceneCluster;
    private ChokMiniCluster loadTestCluster;

    private static void deployIndex(InteractionProtocol protocol, String indexName, URI indexUri) throws InterruptedException {
        DeployClient deployClient1 = new DeployClient(protocol);
        IIndexDeployFuture deployment = deployClient1.addIndex(indexName, indexUri, 1);
        LOG.info("Joining deployment on " + deployment.getClass().getName());
        deployment.joinDeployment();
    }

    @Before
    public void setUp() throws Exception {
        luceneCluster = new ChokMiniCluster(LuceneServer.class, NODE_COUNT_LUCENE, 30000, TestLuceneNodeConfigurationFactory.class, injector.getInstance(ChokFileSystem.Factory.class));
        luceneCluster.start(zk, LUCENE_ZK_ROOT_PATH);

        loadTestCluster = new ChokMiniCluster(SleepServer.class, NODE_COUNT_LOAD_TEST, 40000, TestNodeConfigurationFactory.class, injector.getInstance(ChokFileSystem.Factory.class));
        loadTestCluster.start(zk, LOAD_TEST_ZK_ROOT_PATH);

        TestUtil.waitUntilLeaveSafeMode(luceneCluster.getMaster());
        TestUtil.waitUntilLeaveSafeMode(loadTestCluster.getMaster());
        TestUtil.waitUntilNumberOfLiveNode(luceneCluster.getProtocol(), NODE_COUNT_LUCENE);
        TestUtil.waitUntilNumberOfLiveNode(loadTestCluster.getProtocol(), NODE_COUNT_LOAD_TEST);

        LOG.info("Deploying indices");
        deployIndex(luceneCluster.getProtocol(), "index1", LuceneTestResources.INDEX1.getIndexUri());

        // Verify setup.
        // LOG.info("\n\nLUCENE CLUSTER STRUCTURE:\n");
        // luceneCluster.getProtocol().showStructure(false);
        // LOG.info("\n\nLOADTEST CLUSTER STRUCTURE:\n");
        // loadTestCluster.getProtocol().showStructure(false);
    }

    @After
    public void tearDown() throws Exception {
        luceneCluster.stop(zk, LUCENE_ZK_ROOT_PATH);
        loadTestCluster.stop(zk, LOAD_TEST_ZK_ROOT_PATH);
        zk.cleanupZk();
    }

    @Test
    public void testLuceneLoadTest() throws Exception {
        File resultDir = temporaryFolder.newFolder("results");
        int startRate = 10;
        int endRate = 50;
        int step = 10;
        int runTime = 2000;
        LuceneSearchExecutor queryExecutor = new LuceneSearchExecutor(new String[]{"*"}, new String[]{"foo:bar", "notExists"}, luceneCluster.getZkConfiguration(), 200);
        LoadTestMasterOperation loadTestOperation = new LoadTestMasterOperation(NODE_COUNT_LOAD_TEST, startRate, endRate, step, runTime, queryExecutor, resultDir);
        loadTestOperation.registerCompletion(loadTestCluster.getProtocol());
        loadTestCluster.getProtocol().addMasterOperation(loadTestOperation);
        loadTestOperation.joinCompletion(loadTestCluster.getProtocol());

        assertEquals(2, resultDir.list().length);
        File[] listFiles = resultDir.listFiles();
        File logFile;
        File resultFile;
        if (listFiles[0].getName().contains("-results-")) {
            resultFile = listFiles[0];
            logFile = listFiles[1];
        } else {
            resultFile = listFiles[1];
            logFile = listFiles[0];
        }
        assertThat(TestIoUtil.countLines(logFile), almostEquals(300, 50));
        int iterations = 1 + (endRate - startRate) / step;
        assertEquals(1 + iterations, TestIoUtil.countLines(resultFile));
    }

}
