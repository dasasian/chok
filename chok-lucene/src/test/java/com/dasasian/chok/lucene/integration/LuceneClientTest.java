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
import com.dasasian.chok.client.IDeployClient;
import com.dasasian.chok.client.IndexState;
import com.dasasian.chok.lucene.*;
import com.dasasian.chok.lucene.testutil.LuceneIndexGenerator;
import com.dasasian.chok.lucene.testutil.LuceneTestResources;
import com.dasasian.chok.lucene.testutil.TestLuceneNodeConfigurationFactory;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.testutil.TestUtil;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.util.ChokException;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.UtilModule;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

/**
 * Test for {@link LuceneClient}.
 */
public class LuceneClientTest extends AbstractTest {

    protected static Injector injector = Guice.createInjector(new UtilModule());

    private static final String INDEX1 = "index1";
    private static final String INDEX2 = "index2";
    private static final String INDEX3 = "index3";
    private static Logger LOG = LoggerFactory.getLogger(LuceneClientTest.class);
    @Rule
    public ChokMiniCluster miniCluster = new ChokMiniCluster(LuceneServer.class, 2, 20000, TestLuceneNodeConfigurationFactory.class, injector.getInstance(ChokFileSystem.Factory.class));

    @Test
    public void testAddRemoveIndices() throws Exception {
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        IDeployClient deployClient = new DeployClient(miniCluster.getProtocol());

        int listenerCountBeforeDeploys = miniCluster.getProtocol().getRegisteredListenerCount();

        List<String> indexNames = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            TestIndex testIndex = LuceneIndexGenerator.createTestIndex(temporaryFolder, 2);
            String indexName = testIndex.getIndexName();
            deployClient.addIndex(indexName, testIndex.getIndexUri(), 1, false).joinDeployment();
            indexNames.add(indexName);
        }

        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
        client.search(query, null, 10);

        indexNames.forEach(deployClient::removeIndex);

        for (String indexName : indexNames) {
            while (deployClient.existsIndex(indexName)) {
                Thread.sleep(1000);
            }
        }

        assertEquals(listenerCountBeforeDeploys, miniCluster.getProtocol().getRegisteredListenerCount());

        client.close();
    }

    @Test
    public void testInstantiateClientBeforeIndex() throws Exception {
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        miniCluster.deployIndex(LuceneTestResources.INDEX1, miniCluster.getRunningNodeCount());
        List<Node> nodes = miniCluster.getNodes();
        for (Node node : nodes) {
            TestUtil.waitUntilNodeServesShards(miniCluster.getProtocol(), node.getName(), LuceneTestResources.INDEX1.getShardCount());
        }
        Thread.sleep(2000);

        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
        client.count(query, null);
        client.close();
    }

    @Test
    public void testCount() throws Exception {
        miniCluster.deployIndex(LuceneTestResources.INDEX1, 1);
        ILuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
        final int count = client.count(query, null);
        assertEquals(937, count);
        client.close();
    }

    @Test
    public void testGetDetails() throws Exception {
        miniCluster.deployIndex(LuceneTestResources.INDEX1, 1);
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
        final Hits hits = client.search(query, null, 10);
        assertNotNull(hits);
        assertEquals(10, hits.getHits().size());
        for (final Hit hit : hits.getHits()) {
            final MapWritable details = client.getDetails(hit);
            final Set<Writable> keySet = details.keySet();
            assertFalse(keySet.isEmpty());
            assertNotNull(details.get(new Text("path")));
            assertNotNull(details.get(new Text("category")));
        }
        client.close();
    }

    @Test
    public void testGetDetailsWithFieldNames() throws Exception {
        miniCluster.deployIndex(LuceneTestResources.INDEX1, 1);
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
        final Hits hits = client.search(query, null, 10);
        assertNotNull(hits);
        assertEquals(10, hits.getHits().size());
        for (final Hit hit : hits.getHits()) {
            final MapWritable details = client.getDetails(hit, new String[]{"path"});
            final Set<Writable> keySet = details.keySet();
            assertFalse(keySet.isEmpty());
            assertNotNull(details.get(new Text("path")));
            assertNull(details.get(new Text("category")));
        }
        client.close();
    }

    @Test
    public void testGetBinaryDetails() throws Exception {
        File index = temporaryFolder.newFolder("indexWithBinaryData");
        File indexShard = new File(index, "binaryShard");
        if (!indexShard.mkdirs()) {
            throw new RuntimeException("Unable to create directory " + indexShard.getAbsolutePath());
        }

        String textFieldName = "textField";
        String binaryFieldName = "binaryField";
        String textFieldContent = "sample text";
        byte[] bytesFieldContent = new byte[]{1, 2, 3};

        IndexWriter indexWriter = new IndexWriter(FSDirectory.open(indexShard), new StandardAnalyzer(Version.LUCENE_30), true, MaxFieldLength.UNLIMITED);
        Document document = new Document();
        document.add(new Field(binaryFieldName, bytesFieldContent, Store.YES));
        document.add(new Field(textFieldName, textFieldContent, Store.NO, Index.ANALYZED));
        indexWriter.addDocument(document);
        indexWriter.close(true);
        DeployClient deployClient = new DeployClient(miniCluster.getProtocol());
        IndexState indexState = deployClient.addIndex(index.getName(), index.toURI(), 1, false).joinDeployment();
        assertEquals(IndexState.DEPLOYED, indexState);

        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse(textFieldName + ": " + textFieldContent);
        final Hits hits = client.search(query, new String[]{index.getName()}, 10);
        assertNotNull(hits);
        assertEquals(1, hits.getHits().size());
        final Hit hit = hits.getHits().get(0);
        final MapWritable details = client.getDetails(hit);
        final Set<Writable> keySet = details.keySet();
        assertEquals(1, keySet.size());
        final Writable writable = details.get(new Text(binaryFieldName));
        assertNotNull(writable);
        assertThat(writable, instanceOf(BytesWritable.class));
        BytesWritable bytesWritable = (BytesWritable) writable;
        bytesWritable.setCapacity(bytesWritable.getLength());// getBytes() returns
        // the full array
        assertArrayEquals(bytesFieldContent, bytesWritable.getBytes());
        client.close();
    }

    @Test
    public void testGetDetailsConcurrently() throws Exception {
        miniCluster.deployIndex(LuceneTestResources.INDEX1, 1);
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
        final Hits hits = client.search(query, null, 10);
        assertNotNull(hits);
        assertEquals(10, hits.getHits().size());
        List<MapWritable> detailList = client.getDetails(hits.getHits());
        assertEquals(hits.getHits().size(), detailList.size());
        for (int i = 0; i < detailList.size(); i++) {
            final MapWritable details1 = client.getDetails(hits.getHits().get(i));
            final MapWritable details2 = detailList.get(i);
            assertEquals(details1.entrySet(), details2.entrySet());
            final Set<Writable> keySet = details2.keySet();
            assertFalse(keySet.isEmpty());
            final Writable writable = details2.get(new Text("path"));
            assertNotNull(writable);
        }
        client.close();
    }

    @Test
    public void testSearch() throws Exception {
        deploy3Indices();
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("foo: bar");
        final Hits hits = client.search(query, new String[]{INDEX3, INDEX2});
        assertNotNull(hits);
        hits.getHits().forEach(this::writeToLog);
        assertEquals(8, hits.size());
        assertEquals(8, hits.getHits().size());
        client.close();
    }

    @Test
    public void testFieldSortWithNoResultShard() throws Exception {
        String indexName = "sortIndex";

        File sortIndex = temporaryFolder.newFolder(indexName);
        File sortShard1 = new File(sortIndex, "sortIndex1");
        File sortShard2 = new File(sortIndex, "sortIndex2");
        IndexWriter indexWriter1 = new IndexWriter(FSDirectory.open(sortShard1), new StandardAnalyzer(Version.LUCENE_30), true, MaxFieldLength.UNLIMITED);
        IndexWriter indexWriter2 = new IndexWriter(FSDirectory.open(sortShard2), new StandardAnalyzer(Version.LUCENE_30), true, MaxFieldLength.UNLIMITED);

        Document document = new Document();
        document.add(new Field("text", "abc", Field.Store.YES, Index.NOT_ANALYZED));
        document.add(new NumericField("timesort", Field.Store.YES, false).setLongValue(1234567890123L));
        indexWriter1.addDocument(document);
        indexWriter1.close();

        document = new Document();
        document.add(new Field("text", "abc2", Field.Store.YES, Index.NOT_ANALYZED));
        document.add(new NumericField("timesort", Field.Store.YES, false).setLongValue(1234567890123L));
        indexWriter2.addDocument(document);
        indexWriter2.close();

        miniCluster.deployIndex(indexName, sortIndex.toURI(), 1);

        // query and compare results
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        Sort sort = new Sort(new SortField[]{new SortField("timesort", SortField.LONG)});

        // query both documents
        Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("text:ab*");
        Hits hits = client.search(query, null, 20, sort);
        assertEquals(2, hits.size());

        // query only one document
        query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("text:abc2");
        hits = client.search(query, null, 20, sort);
        assertEquals(1, hits.size());

        // query only one document on one node
        miniCluster.shutdownNode(0);
        TestUtil.waitUntilIndexBalanced(miniCluster.getProtocol(), indexName);
        query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("text:abc2");
        hits = client.search(query, null, 20, sort);
        assertEquals(1, hits.size());
        client.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSortedSearch() throws Exception {
        // write and deploy test index
        String queryTerm = "2";
        String textFieldName = "textField";
        File sortIndex = temporaryFolder.newFolder("sortIndex2");
        File sortShard = new File(sortIndex, "sortShard");
        String sortFieldName = "sortField";
        IndexWriter indexWriter = new IndexWriter(FSDirectory.open(sortShard), new StandardAnalyzer(Version.LUCENE_30), true, MaxFieldLength.UNLIMITED);
        for (int i = 0; i < 20; i++) {
            Document document = new Document();
            document.add(new Field(sortFieldName, "" + i, Store.NO, Index.NOT_ANALYZED));
            String textField = "sample text";
            if (i % 2 == 0) {// produce some different scores
                for (int j = 0; j < i; j++) {
                    textField += " " + queryTerm;
                }
            }
            document.add(new Field(textFieldName, textField, Store.NO, Index.ANALYZED));
            indexWriter.addDocument(document);
        }
        indexWriter.close(true);
        DeployClient deployClient = new DeployClient(miniCluster.getProtocol());
        IndexState indexState = deployClient.addIndex(sortIndex.getName(), sortIndex.toURI(), 1, false).joinDeployment();
        assertEquals(IndexState.DEPLOYED, indexState);

        // query and compare results
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse(textFieldName + ": " + queryTerm);
        Sort sort = new Sort(new SortField[]{new SortField(sortFieldName, SortField.INT)});
        final Hits hits = client.search(query, new String[]{sortIndex.getName()}, 20, sort);
        assertNotNull(hits);
        List<Hit> hitsList = hits.getHits();
        hitsList.forEach(this::writeToLog);
        assertEquals(9, hits.size());
        assertEquals(9, hitsList.size());
        assertEquals(1, hitsList.get(0).getSortFields().length);
        for (int i = 0; i < hitsList.size() - 1; i++) {
            int compareTo = hitsList.get(i).getSortFields()[0].compareTo(hitsList.get(i + 1).getSortFields()[0]);
            assertTrue("results not after field", compareTo == 0 || compareTo == -1);
        }
        client.close();
    }

    @Test
    public void testSearchLimit() throws Exception {
        deploy3Indices();
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("foo: bar");
        final Hits hits = client.search(query, new String[]{INDEX3, INDEX2}, 1);
        assertNotNull(hits);
        hits.getHits().forEach(this::writeToLog);
        assertEquals(8, hits.size());
        assertEquals(1, hits.getHits().size());
        for (final Hit hit : hits.getHits()) {
            LOG.info(hit.getNode() + " -- " + hit.getScore() + " -- " + hit.getDocId());
        }
        client.close();
    }

    @Test
    public void testChok20SearchLimitMaxNumberOfHits() throws Exception {
        miniCluster.deployIndex(LuceneTestResources.INDEX1, miniCluster.getRunningNodeCount());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("foo: bar");
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Hits expectedHits = client.search(query, null, 4);
        assertNotNull(expectedHits);
        LOG.info("Expected hits:");
        expectedHits.getHits().forEach(this::writeToLog);
        assertEquals(4, expectedHits.getHits().size());

        for (int i = 0; i < 100; i++) {
            // Now we redo the search, but limit the max number of hits. We expect the
            // same ordering of hits.
            for (int maxHits = 1; maxHits < expectedHits.size() + 1; maxHits++) {
                final Hits hits = client.search(query, null, maxHits);
                assertNotNull(hits);
                assertEquals(maxHits, hits.getHits().size());
                for (int j = 0; j < hits.getHits().size(); j++) {
                    // writeToLog("expected: ", expectedHits.getHits().get(j));
                    // writeToLog("actual : ", hits.getHits().get(j));
                    assertEquals(expectedHits.getHits().get(j).getScore(), hits.getHits().get(j).getScore(), 0.0);
                }
            }
        }
        client.close();
    }

    @Test
    public void testSearchSimiliarity() throws Exception {
        deploy3Indices();
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("foo: bar");
        final Hits hits = client.search(query, new String[]{INDEX2});
        assertNotNull(hits);
        assertEquals(4, hits.getHits().size());
        for (final Hit hit : hits.getHits()) {
            LOG.info(hit.getNode() + " -- " + hit.getScore() + " -- " + hit.getDocId());
        }
        client.close();
    }

    @Test
    public void testNonExistentShard() throws Exception {
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("foo: bar");
        ILuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        try {
            client.search(query, new String[]{"doesNotExist"});
            fail("Should have failed.");
        } catch (ChokException e) {
            assertEquals("Index [pattern(s)] '[doesNotExist]' do not match to any deployed index: []", e.getMessage());
        }
        client.close();
    }

    @Test
    public void testIndexPattern() throws Exception {
        deploy3Indices();
        ILuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol());
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("foo: bar");
        final Hits hits = client.search(query, new String[]{"index[2-3]+"});
        assertNotNull(hits);
        hits.getHits().forEach(this::writeToLog);
        assertEquals(8, hits.size());
        assertEquals(8, hits.getHits().size());
        client.close();
    }

    @Test
    public void testNumDocGreaterMaxInteger_CHOK_140() throws Exception {
        miniCluster.deployIndex(LuceneTestResources.INDEX1, 1);
        LuceneClient client = new LuceneClient(miniCluster.createInteractionProtocol()) {
            @Override
            protected DocumentFrequencyWritable getDocFrequencies(Query q, String[] indexNames) throws ChokException {
                DocumentFrequencyWritable docFreq = new DocumentFrequencyWritable();
                docFreq.put("foo", "bar", 23);
                docFreq.addNumDocs(Integer.MAX_VALUE);
                docFreq.addNumDocs(23);
                // docFreq.
                return docFreq;
            }
        };
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("foo: bar");
        client.search(query, null, 10, null);
        // client.search(query, new String[] { indexName }, 10, new Sort(new
        // SortField("foo", SortField.STRING)));
        client.close();
    }

    @Test
    public void testFilteredSearch() throws Exception {
        // write and deploy test index
        File filterIndex = temporaryFolder.newFolder("filterIndex");
        File filterShard = new File(filterIndex, "filterShard");
        String textFieldName = "textField";
        String filterFieldName = "filterField";
        IndexWriter indexWriter = new IndexWriter(FSDirectory.open(filterShard), new StandardAnalyzer(Version.LUCENE_30), true, MaxFieldLength.UNLIMITED);
        for (int i = 0; i < 100; i++) {
            Document document = new Document();
            document.add(new Field(textFieldName, "sample " + i, Store.YES, Index.NOT_ANALYZED));
            document.add(new Field(filterFieldName, "" + (i % 10), Store.YES, Index.NOT_ANALYZED));
            indexWriter.addDocument(document);
        }
        indexWriter.close(true);

        DeployClient deployClient = new DeployClient(miniCluster.createInteractionProtocol());
        IndexState indexState = deployClient.addIndex(filterIndex.getName(), filterIndex.toURI(), 1, false).joinDeployment();
        assertEquals(IndexState.DEPLOYED, indexState);

        // build filter for terms in set {i | (i % 10) == 3}.
        LuceneClient client = new LuceneClient(miniCluster.getZkConfiguration());
        TermQuery filterQuery = new TermQuery(new Term(filterFieldName, "3"));
        QueryWrapperFilter filter = new QueryWrapperFilter(filterQuery);
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse(textFieldName + ":" + "sample*3");

        final Hits hits = client.search(query, new String[]{filterIndex.getName()}, 100, null, filter);
        assertNotNull(hits);
        List<Hit> hitsList = hits.getHits();
        hitsList.forEach(this::writeToLog);
        assertEquals(10, hits.size());
        assertEquals(10, hitsList.size());

        // check that returned results conform to the filter
        for (final Hit hit : hitsList) {
            MapWritable mw = client.getDetails(hit);
            Text text = (Text) mw.get(new Text("textField"));
            assertNotNull(text);
            String[] parts = text.toString().split(" ");
            assertTrue(parts.length == 2);
            int num = Integer.valueOf(parts[1]);
            assertTrue((num % 10) == 3);
        }
        client.close();
    }

    private void writeToLog(Hit hit) {
        LOG.info(hit.getNode() + " -- " + hit.getShard() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }

    private void deploy3Indices() throws Exception {
        DeployClient deployClient = new DeployClient(miniCluster.getProtocol());
        deployClient.addIndex(INDEX1, LuceneTestResources.INDEX1.getIndexUri(), 1, false).joinDeployment();
        deployClient.addIndex(INDEX2, LuceneTestResources.INDEX1.getIndexUri(), 1, false).joinDeployment();
        deployClient.addIndex(INDEX3, LuceneTestResources.INDEX1.getIndexUri(), 1, false).joinDeployment();
    }

}
