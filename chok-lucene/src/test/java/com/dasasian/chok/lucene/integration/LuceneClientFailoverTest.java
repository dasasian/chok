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

import com.dasasian.chok.client.NodeProxyManager;
import com.dasasian.chok.client.ShardAccessException;
import com.dasasian.chok.lucene.Hits;
import com.dasasian.chok.lucene.LuceneClient;
import com.dasasian.chok.lucene.LuceneServer;
import com.dasasian.chok.lucene.testutil.LuceneTestResources;
import com.dasasian.chok.lucene.testutil.TestLuceneNodeConfigurationFactory;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class LuceneClientFailoverTest extends AbstractTest {

    @Rule
    public ChokMiniCluster miniCluster = new ChokMiniCluster(LuceneServer.class, 2, 20000, TestLuceneNodeConfigurationFactory.class);

    @Before
    public void deployIndex() throws Exception {
        miniCluster.deployIndex(LuceneTestResources.INDEX1, miniCluster.getRunningNodeCount());
    }

    @Test
    public void testSearchAndCount_NodeProxyDownAfterClientInitialization() throws Exception {
        // start search client
        LuceneClient luceneClient = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
        assertSearchResults(10, luceneClient.search(query, null, 10));

        // shutdown proxy of node1
        miniCluster.getNode(0).getRpcServer().stop();

        System.out.println("=========================");
        assertSearchResults(10, luceneClient.search(query, null, 10));
        assertEquals(937, luceneClient.count(query, null));
        assertSearchResults(10, luceneClient.search(query, null, 10));
        assertEquals(937, luceneClient.count(query, null));
        // search 2 time to ensure we get all availible nodes
        System.out.println("=========================");
        //        miniCluster.shutdownNode(0);
        luceneClient.close();
    }

    @Test
    public void testGetDetails_NodeProxyDownAfterClientInitialization() throws Exception {
        LuceneClient luceneClient = new LuceneClient(miniCluster.createInteractionProtocol());
        ((NodeProxyManager) luceneClient.getClient().getProxyManager()).setSuccessiveProxyFailuresBeforeReestablishing(1);
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
        Hits hits = luceneClient.search(query, null, 10);

        // shutdown proxy of node1
        System.out.println("=========================");
        if (miniCluster.getNode(0).getName().equals(hits.getHits().get(0).getNode())) {
            miniCluster.shutdownNodeRpc(0);
        } else {
            miniCluster.shutdownNodeRpc(1);
        }
        assertFalse(luceneClient.getDetails(hits.getHits().get(0)).isEmpty());
        assertFalse(luceneClient.getDetails(hits.getHits().get(0)).isEmpty());
        // search 2 time to ensure we get all available nodes
        System.out.println("=========================");
        //        miniCluster.shutdownNodes();
        luceneClient.close();
    }

    @Test
    public void testAllNodeProxyDownAfterClientInitialization() throws Exception {
        LuceneClient luceneClient = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
        for (int i = 0; i < miniCluster.getRunningNodeCount(); i++) {
            miniCluster.shutdownNodeRpc(i);
        }

        System.out.println("=========================");
        try {
            luceneClient.search(query, null, 10);
            fail("should throw exception");
        } catch (ShardAccessException e) {
            // expected
        }
        System.out.println("=========================");
        //        shutdownNodes();
        luceneClient.close();
    }

    private void assertSearchResults(int expectedResults, Hits hits) {
        assertNotNull(hits);
        assertEquals(expectedResults, hits.getHits().size());
    }
}
