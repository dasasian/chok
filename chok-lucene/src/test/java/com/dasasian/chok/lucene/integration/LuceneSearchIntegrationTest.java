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

import com.dasasian.chok.client.Client;
import com.dasasian.chok.lucene.Hits;
import com.dasasian.chok.lucene.ILuceneClient;
import com.dasasian.chok.lucene.LuceneClient;
import com.dasasian.chok.lucene.LuceneServer;
import com.dasasian.chok.lucene.testutil.LuceneTestResources;
import com.dasasian.chok.lucene.testutil.TestLuceneNodeConfigurationFactory;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.util.StringUtil;
import com.dasasian.chok.util.ZkConfiguration;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class LuceneSearchIntegrationTest extends AbstractTest {

    private static final int SEARCH_THREAD_COUNT = 9;
    private static final int QUERY_TIME = 5000;

    @ClassRule
    public static ChokMiniCluster miniCluster = new ChokMiniCluster(LuceneServer.class, 5, 20000, TestLuceneNodeConfigurationFactory.class);

    @BeforeClass
    public static void deployIndexes() throws Exception {
        miniCluster.deployIndexes(LuceneTestResources.INDEX1, 3, 3);
    }

    @Test
    public void testSearch() throws Exception {
        long startTime = System.currentTimeMillis();

        // start search threads
        int expectedHitCount = 12;
        SearchThread searchThread = new SearchThread(miniCluster.getZkConfiguration(), expectedHitCount);
        searchThread.start();

        Thread.sleep(QUERY_TIME);
        searchThread.interrupt();
        searchThread.join();

        checkResults(startTime, QUERY_TIME, searchThread.getFiredQueryCount(), searchThread.getUnexpectedResultCount(), searchThread.getThrownExceptions());
    }

    @Test
    public void testMultithreadedSearchWithMultipleClients() throws Exception {
        long startTime = System.currentTimeMillis();

        // start search threads
        int expectedHitCount = 12;
        SearchThread[] searchThreads = new SearchThread[SEARCH_THREAD_COUNT];
        for (int i = 0; i < searchThreads.length; i++) {
            searchThreads[i] = new SearchThread(miniCluster.getZkConfiguration(), expectedHitCount);
            searchThreads[i].start();
        }
        Thread.sleep(QUERY_TIME);

        // stop everything
        long firedQueries = 0;
        long unexpectedResultCount = 0;
        List<Exception> exceptions = Lists.newArrayList();
        for (SearchThread searchThread : searchThreads) {
            searchThread.interrupt();
            searchThread.join();
            firedQueries += searchThread.getFiredQueryCount();
            unexpectedResultCount += searchThread.getUnexpectedResultCount();
            exceptions.addAll(searchThread.getThrownExceptions());
        }

        checkResults(startTime, QUERY_TIME, firedQueries, unexpectedResultCount, exceptions);
    }

    @Test
    public void testSearchWhileStartingAndStoppingNodes() throws Exception {
        long startTime = System.currentTimeMillis();

        // start search threads
        int expectedHitCount = 12;
        SearchThread searchThread = new SearchThread(miniCluster.getZkConfiguration(), expectedHitCount);
        searchThread.start();

        startAndStopNodes(QUERY_TIME);

        // stop everything
        searchThread.interrupt();
        searchThread.join();
        checkResults(startTime, QUERY_TIME, searchThread.getFiredQueryCount(), searchThread.getUnexpectedResultCount(), searchThread.getThrownExceptions());
    }

    @Test
    public void testMultithreadedSearchWithOneClientWhileStartingAndStoppingNodes() throws Exception {
        long startTime = System.currentTimeMillis();

        // start search threads
        int expectedHitCount = 12;
        SearchThread[] searchThreads = new SearchThread[SEARCH_THREAD_COUNT];
        ILuceneClient searchClient = new LuceneClient(miniCluster.getZkConfiguration());
        for (int i = 0; i < searchThreads.length; i++) {
            searchThreads[i] = new SearchThread(searchClient, expectedHitCount);
            searchThreads[i].start();
        }

        startAndStopNodes(QUERY_TIME);

        // stop everything
        long firedQueries = 0;
        long unexpectedResultCount = 0;
        List<Exception> exceptions = Lists.newArrayList();
        for (SearchThread searchThread : searchThreads) {
            searchThread.interrupt();
            searchThread.join();
            firedQueries += searchThread.getFiredQueryCount();
            unexpectedResultCount += searchThread.getUnexpectedResultCount();
            exceptions.addAll(searchThread.getThrownExceptions());
        }
        searchClient.close();

        checkResults(startTime, QUERY_TIME, firedQueries, unexpectedResultCount, exceptions);
    }

    private void startAndStopNodes(long queryTime) throws Exception {
        LuceneClient luceneClient = new LuceneClient(miniCluster.getProtocol());
        Client client = luceneClient.getClient();

        Thread.sleep(queryTime / 4);
        System.out.println("-----------------------SHUTDOWN NODE1: " + miniCluster.getProtocol().getShard2NodesMap(miniCluster.getProtocol().getShard2NodeShards()));
        System.out.println(client.getSelectionPolicy().toString());
        miniCluster.shutdownNode(0);

        Thread.sleep(queryTime / 4);
        System.out.println("-----------------------SHUTDOWN NODE2: " + miniCluster.getProtocol().getShard2NodesMap(miniCluster.getProtocol().getShard2NodeShards()));
        System.out.println(client.getSelectionPolicy().toString());
        miniCluster.shutdownNode(0);

        Thread.sleep(queryTime / 4);
        System.out.println("-----------------------START NEW NODE: " + miniCluster.getProtocol().getShard2NodesMap(miniCluster.getProtocol().getShard2NodeShards()));
        System.out.println(client.getSelectionPolicy().toString());
        miniCluster.startAdditionalNode();

        System.out.println("-----------------------SHUTDOWN NEW NODE: " + miniCluster.getProtocol().getShard2NodesMap(miniCluster.getProtocol().getShard2NodeShards()));
        System.out.println(client.getSelectionPolicy().toString());
        Thread.sleep(queryTime / 2);
        miniCluster.shutdownNode(miniCluster.getRunningNodeCount() - 1);

//        luceneClient.close();
    }

    private void checkResults(long startTime, long queryTime, long firedQueries, long unexpectedResultCount, List<Exception> exceptions) throws Exception {
        // print results
        System.out.println("===========================================");
        System.out.println("Results of " + printMethodNames.getCurrentMethodName() + ":");
        System.out.println("search time: " + StringUtil.formatTimeDuration(queryTime));
        System.out.println("fired queries: " + firedQueries);
        System.out.println("wrong results: " + unexpectedResultCount);
        System.out.println("exceptions: " + exceptions.size());
        System.out.println("execution took: " + StringUtil.formatTimeDuration(System.currentTimeMillis() - startTime));
        System.out.println("===========================================");

        // assert results
        if (!exceptions.isEmpty()) {
            throw new IllegalStateException(exceptions.size() + " exception during search", exceptions.get(0));
        }
        assertEquals("wrong hit count", 0, unexpectedResultCount);
    }

    protected static class SearchThread extends Thread {

        private static Logger LOG = Logger.getLogger(SearchThread.class);
        private final long expectedTotalHitCount;
        private volatile boolean stopped;
        private ZkConfiguration zkConfiguration;
        private List<Exception> thrownExceptions = Lists.newArrayList();
        private long firedQueryCount;
        private long unexpectedResultCount;
        private ILuceneClient client;

        public SearchThread(ZkConfiguration zkConfiguration, long expectedTotalHitCount) {
            this.zkConfiguration = zkConfiguration;
            this.expectedTotalHitCount = expectedTotalHitCount;
        }

        public SearchThread(ILuceneClient client, long expectedTotalHitCount) {
            this.client = client;
            this.expectedTotalHitCount = expectedTotalHitCount;
        }

        @Override
        public void run() {
            try {
                ILuceneClient client;
                if (this.client == null) {
                    if (zkConfiguration != null) {
                        client = new LuceneClient(zkConfiguration);
                    } else {
                        client = new LuceneClient();
                    }
                } else {
                    client = this.client;
                }
                while (!stopped) {
                    final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("foo:bar");
                    Hits hits = client.search(query, new String[]{"*"});
                    firedQueryCount++;
                    if (hits.size() != expectedTotalHitCount) {
                        unexpectedResultCount++;
                        LOG.error("expected " + expectedTotalHitCount + " hits but got " + hits.size());
                    }
                }
                if (this.client == null) {
                    client.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                thrownExceptions.add(e);
            }
        }

        @Override
        public void interrupt() {
            stopped = true;
            // jz: we don't call super.interrupt() since the client swallows such
            // InterruptedException's
        }

        public List<Exception> getThrownExceptions() {
            return thrownExceptions;
        }

        public long getFiredQueryCount() {
            return firedQueryCount;
        }

        public long getUnexpectedResultCount() {
            return unexpectedResultCount;
        }

    }
}
