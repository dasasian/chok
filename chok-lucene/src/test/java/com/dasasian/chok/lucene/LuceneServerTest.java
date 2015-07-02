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
package com.dasasian.chok.lucene;

import com.dasasian.chok.lucene.testutil.LuceneTestResources;
import com.dasasian.chok.lucene.testutil.TestLuceneNodeConfigurationFactory;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.testutil.mockito.ChainedAnswer;
import com.dasasian.chok.testutil.mockito.SleepingAnswer;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.NodeConfiguration;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LuceneServerTest extends AbstractTest {

    public final TestLuceneNodeConfigurationFactory nodeConfigurationFactory = new TestLuceneNodeConfigurationFactory(temporaryFolder);
    // query parser
    private QueryParser queryParser = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer());

    @Test
    public void testPriorityQueue() throws Exception {
        // tests some simple PriorityQueue behavior
        LuceneServer.ChokHitQueue queue = new LuceneServer.ChokHitQueue(2);
        Hit hit1 = new Hit("shard", "node", 1f, 1);
        Hit hit2 = new Hit("shard", "node", 2f, 1);
        Hit hit3 = new Hit("shard", "node", 3f, 1);
        Hit hit4 = new Hit("shard", "node", 4f, 1);

        assertTrue(queue.insert(hit1));
        assertTrue(queue.insert(hit2));
        assertTrue(queue.insert(hit3));
        assertTrue(queue.insert(hit4));

        assertEquals(2, queue.size());
        assertSame(hit3, queue.pop());
        assertSame(hit4, queue.pop());
    }

    @Test
    public void testPriorityQueue_sameScore() throws Exception {
        LuceneServer.ChokHitQueue queue = new LuceneServer.ChokHitQueue(2);
        Hit hit1 = new Hit("shard", "node", 1f, 1);
        Hit hit2 = new Hit("shard", "node", 1f, 2);
        Hit hit3 = new Hit("shard", "node", 1f, 3);

        assertTrue(queue.insert(hit1));
        assertTrue(queue.insert(hit2));
        assertTrue(queue.insert(hit3));
        assertEquals(2, queue.size());

        // Queue should return documents with the smaller document ids first if
        // documents have the same score.
        assertSame(hit2, queue.pop());
        assertSame(hit3, queue.pop());
    }

    @Test
    public void testConfiguration() throws Exception {
        // no property in configuration
        LuceneServer server = new LuceneServer();
        server.init("server", nodeConfigurationFactory.getConfiguration());
        assertEquals("server", server.getNodeName());
        assertEquals(0.75f, server.getTimeoutPercentage(), 0.5);
        server.shutdown();

        // property in configuration
        server = new LuceneServer();
        server.init("server", nodeConfigurationFactory.getConfiguration());
        assertEquals("server", server.getNodeName());
        assertEquals(0.5f, server.getTimeoutPercentage(), 0.5);
        server.shutdown();
    }

    @Test
    public void testSearch_Timeout() throws Exception {
        int clientTimeout = 10000;
        // disabled timeout
        NodeConfiguration luceneNodeConfiguration = nodeConfigurationFactory.getConfiguration(DefaultSearcherFactory.class, 0.0f);
        LuceneServer server = new LuceneServer();
        server.init("server", luceneNodeConfiguration);
        String[] shardNames = addIndexShards(server, LuceneTestResources.INDEX1);

        QueryWritable queryWritable = new QueryWritable(parseQuery("foo: b*"));
        DocumentFrequencyWritable freqs = server.getDocFreqs(queryWritable, shardNames);
        HitsMapWritable result = server.search(queryWritable, freqs, shardNames, clientTimeout, 1000);
        assertEquals(4, result.getHitList().size());
        server.shutdown();

        // timeout - success
        luceneNodeConfiguration = nodeConfigurationFactory.getConfiguration(DefaultSearcherFactory.class, 0.5f);
        server = new LuceneServer();
        server.init("server", luceneNodeConfiguration);
        addIndexShards(server, LuceneTestResources.INDEX1);
        freqs = server.getDocFreqs(queryWritable, shardNames);
        result = server.search(queryWritable, freqs, shardNames, clientTimeout, 1000);
        assertEquals(4, result.getHitList().size());
        server.shutdown();

        // timeout - failure
        final long serverTimeout = 100;
        final DefaultSearcherFactory seacherFactory = new DefaultSearcherFactory();
        ISearcherFactory mockSeacherFactory = mock(ISearcherFactory.class);
        final AtomicInteger shardsWithTimeoutCount = new AtomicInteger();
        when(mockSeacherFactory.createSearcher(anyString(), any(Path.class))).thenAnswer(invocation -> {
            final IndexSearcher indexSearcher = seacherFactory.createSearcher((String) invocation.getArguments()[0], (Path) invocation.getArguments()[1]);
            synchronized (shardsWithTimeoutCount) {
                if (shardsWithTimeoutCount.intValue() >= 2) {
                    // 2 from 4 shards will get tiemout
                    return indexSearcher;
                }
                shardsWithTimeoutCount.incrementAndGet();
            }
            IndexSearcher indexSearcherSpy = spy(indexSearcher);
            doAnswer(new ChainedAnswer(new SleepingAnswer(serverTimeout * 2), new CallsRealMethods())).when(indexSearcherSpy).search(any(Weight.class), any(Filter.class), any(Collector.class));
            return indexSearcherSpy;
        });

        luceneNodeConfiguration = nodeConfigurationFactory.getConfiguration(0.01f);
        server = new LuceneServer();
        server.init("server", luceneNodeConfiguration);
        server.setSearcherFactory(mockSeacherFactory);
        assertEquals(serverTimeout, server.getCollectorTimeout(clientTimeout));
        addIndexShards(server, LuceneTestResources.INDEX1);
        freqs = server.getDocFreqs(queryWritable, shardNames);
        result = server.search(queryWritable, freqs, shardNames, clientTimeout, 1000);
        assertTrue(result.getHitList().size() < 5);
        assertTrue(result.getHitList().size() >= 1);
        server.shutdown();
    }

    private Query parseQuery(String s) throws ParseException {
        return queryParser.parse(s);
    }

    private String[] addIndexShards(LuceneServer server, TestIndex testIndex) throws IOException {
        List<File> shards = testIndex.getShardFiles();
        String[] shardNames = new String[shards.size()];
        int i = 0;
        for (File shard : shards) {
            String shardName = shard.getName();
            server.addShard(shardName, shard.toPath());
            shardNames[i++] = shardName;
        }
        return shardNames;
    }

    @Test
    public void testSearch_MultiThread() throws Exception {
        LuceneNodeConfiguration luceneNodeConfiguration = nodeConfigurationFactory.getConfiguration(DefaultSearcherFactory.class, 0.75f);
        LuceneServer server = new LuceneServer();
        server.init("ls", luceneNodeConfiguration);
        String[] shardNames = addIndexShards(server, LuceneTestResources.INDEX1);

        QueryWritable writable = new QueryWritable(parseQuery("foo: bar"));
        DocumentFrequencyWritable freqs = server.getDocFreqs(writable, shardNames);

        ExecutorService es = Executors.newFixedThreadPool(100);
        List<Future<HitsMapWritable>> tasks = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            QueryClient client = new QueryClient(server, freqs, writable, shardNames);
            Future<HitsMapWritable> future = es.submit(client);
            tasks.add(future);
        }
        HitsMapWritable last = null;
        for (Future<HitsMapWritable> future : tasks) {
            HitsMapWritable hitsMapWritable = future.get();
            if (last == null) {
                last = hitsMapWritable;
            } else {
                Assert.assertEquals(last.getTotalHits(), hitsMapWritable.getTotalHits());
                float lastScore = last.getHitList().get(0).getScore();
                float currentScore = hitsMapWritable.getHitList().get(0).getScore();
                Assert.assertEquals(lastScore, currentScore, 0.01);
            }
        }
        server.shutdown();
    }

    private static class QueryClient implements Callable<HitsMapWritable> {

        private LuceneServer _server;
        private QueryWritable _query;
        private DocumentFrequencyWritable _freqs;
        private String[] _shards;

        public QueryClient(LuceneServer server, DocumentFrequencyWritable freqs, QueryWritable query, String[] shards) {
            _server = server;
            _freqs = freqs;
            _query = query;
            _shards = shards;
        }

        @Override
        public HitsMapWritable call() throws Exception {
            return _server.search(_query, _freqs, _shards, 10000, 2);
        }

    }
}
