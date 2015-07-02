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
import com.dasasian.chok.lucene.ILuceneClient;
import com.dasasian.chok.lucene.LuceneClient;
import com.dasasian.chok.lucene.LuceneServer;
import com.dasasian.chok.lucene.testutil.LuceneTestResources;
import com.dasasian.chok.lucene.testutil.TestLuceneNodeConfigurationFactory;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.UtilModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.junit.ClassRule;
import org.junit.Test;

public class LuceneSearchPerformanceTest extends AbstractTest {

    protected static Injector injector = Guice.createInjector(new UtilModule());

    @ClassRule
    public static ChokMiniCluster miniCluster = new ChokMiniCluster(LuceneServer.class, 2, 20000, TestLuceneNodeConfigurationFactory.class, injector.getInstance(ChokFileSystem.Factory.class));

    @Test
    public void measureSearchPerformance() throws Exception {
        DeployClient deployClient = new DeployClient(miniCluster.getProtocol());
        deployClient.addIndex("index1", LuceneTestResources.INDEX1.getIndexUri(), 1).joinDeployment();
        deployClient.addIndex("index2", LuceneTestResources.INDEX2.getIndexUri(), 1).joinDeployment();

        final ILuceneClient client = new LuceneClient(miniCluster.getZkConfiguration());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("foo: bar");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            client.search(query, new String[]{"index2", "index1"});
        }
        System.out.println("search took: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            client.count(query, new String[]{"index2", "index1"});
        }
        System.out.println("count took: " + (System.currentTimeMillis() - start));
        client.close();
    }

}
