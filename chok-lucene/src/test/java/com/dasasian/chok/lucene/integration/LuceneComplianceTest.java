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
import com.dasasian.chok.lucene.testutil.TestLuceneNodeConfigurationFactory;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.UtilModule;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.io.WritableComparable;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test common lucene operations on sharded indices through chok interface
 * versus pure lucene interface one big index.
 */
public class LuceneComplianceTest extends AbstractTest {

    protected static Injector injector = Guice.createInjector(new UtilModule());

    // index related fields
    private static final String FIELD_NAME = "text";
    @ClassRule
    public static ChokMiniCluster miniCluster = new ChokMiniCluster(LuceneServer.class, 2, 20000, TestLuceneNodeConfigurationFactory.class, injector.getInstance(ChokFileSystem.Factory.class));
    private File chokIndex;
    private File luceneIndex;
    private List<Document> documents1;
    private List<Document> documents2;
    private ILuceneClient luceneClient;

    private static List<Document> createSimpleNumberDocuments(String textFieldName, int count) {
        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String fieldContent = i + " " + (count - i);
            if (i % 2 == 0) {
                fieldContent += " 2";
            } else {
                fieldContent += " 1";
            }
            Document document = new Document();
            document.add(new Field(textFieldName, fieldContent, Store.NO, Index.ANALYZED));
            documents.add(document);
        }
        return documents;
    }

    @SafeVarargs
    private static List<Document> combineDocuments(List<Document>... documentLists) {
        return Lists.newArrayList(Iterables.concat(documentLists));
    }

    private static void writeIndex(File file, List<Document> documents) throws IOException {
        file.mkdirs();
        assertTrue(file.exists());
        IndexWriter indexWriter = new IndexWriter(FSDirectory.open(file), new StandardAnalyzer(Version.LUCENE_30), true, MaxFieldLength.UNLIMITED);
        for (Document document : documents) {
            indexWriter.addDocument(document);
        }
        indexWriter.close(true);

    }

    private static void deployIndexToChok(IDeployClient deployClient, File file, int replicationLevel) throws InterruptedException {
        IndexState indexState = deployClient.addIndex(file.getName(), file.toURI(), replicationLevel).joinDeployment();
        assertEquals(IndexState.DEPLOYED, indexState);
    }

    @Before
    public void setUp() throws Exception {
        IDeployClient _deployClient = new DeployClient(miniCluster.getProtocol());
        // generate 3 index (2 shards + once combined index)
        luceneIndex = temporaryFolder.newFolder();//LuceneClientTest.class.getSimpleName(), "-lucene");
        chokIndex = temporaryFolder.newFolder();// File.createTempFile(LuceneClientTest.class.getSimpleName(), "-chok");
        chokIndex.delete();
        luceneIndex.delete();
        File shard1 = new File(chokIndex, "shard1");
        File shard2 = new File(chokIndex, "shard2");
        documents1 = createSimpleNumberDocuments(FIELD_NAME, 123);
        documents2 = createSimpleNumberDocuments(FIELD_NAME, 78);

        writeIndex(shard1, documents1);
        writeIndex(shard2, documents2);
        writeIndex(luceneIndex, combineDocuments(documents1, documents2));

        // deploy 2 indexes to chok
        deployIndexToChok(_deployClient, chokIndex, 2);
        luceneClient = new LuceneClient(miniCluster.getZkConfiguration());
    }

    @After
    public void tearDown() {
        luceneClient.close();
    }

    @Test
    public void testScoreSort() throws Exception {
        // query and compare
        IndexSearcher indexSearcher = new IndexSearcher(FSDirectory.open(luceneIndex.getAbsoluteFile()));
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "0", null);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "1", null);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "2", null);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "15", null);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "23", null);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "2 23", null);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "nothing", null);
    }

    @Test
    public void testFieldSort() throws Exception {
        // query and compare (auto types)
        IndexSearcher indexSearcher = new IndexSearcher(FSDirectory.open(luceneIndex.getAbsoluteFile()));
        Sort sort = new Sort(new SortField[]{new SortField(FIELD_NAME, SortField.LONG)});
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "0", sort);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "1", sort);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "2", sort);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "15", sort);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "23", sort);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "2 23", sort);
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "nothing", sort);

        // check for explicit types
        sort = new Sort(new SortField[]{new SortField(FIELD_NAME, SortField.BYTE)});
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "1", sort);
        sort = new Sort(new SortField[]{new SortField(FIELD_NAME, SortField.INT)});
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "1", sort);
        sort = new Sort(new SortField[]{new SortField(FIELD_NAME, SortField.LONG)});
        checkQueryResults(indexSearcher, chokIndex.getName(), FIELD_NAME, "1", sort);
    }

    private void checkQueryResults(IndexSearcher indexSearcher, String chokIndexName, String fieldName, String queryTerm, Sort sort) throws Exception {
        // check all documents
        checkQueryResults(indexSearcher, chokIndexName, fieldName, queryTerm, Short.MAX_VALUE, sort);

        // check top n documents
        checkQueryResults(indexSearcher, chokIndexName, fieldName, queryTerm, (documents1.size() + documents2.size()) / 2, sort);
    }

    @SuppressWarnings("unchecked")
    private void checkQueryResults(IndexSearcher indexSearcher, String chokIndexName, String fieldName, String queryTerm, int resultCount, Sort sort) throws Exception {
        // final Query query = new QueryParser("", new
        // KeywordAnalyzer()).parse(fieldName + ": " + queryTerm);
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse(fieldName + ": " + queryTerm);
        final TopDocs searchResultsLucene;
        final Hits searchResultsChok;
        if (sort == null) {
            searchResultsLucene = indexSearcher.search(query, resultCount);
            searchResultsChok = luceneClient.search(query, new String[]{chokIndexName}, resultCount);
        } else {
            searchResultsLucene = indexSearcher.search(query, null, resultCount, sort);
            searchResultsChok = luceneClient.search(query, new String[]{chokIndexName}, resultCount, sort);
        }

        assertEquals(searchResultsLucene.totalHits, searchResultsChok.size());

        ScoreDoc[] scoreDocs = searchResultsLucene.scoreDocs;
        List<Hit> hits = searchResultsChok.getHits();
        if (sort == null) {
            for (int i = 0; i < scoreDocs.length; i++) {
                assertEquals(scoreDocs[i].score, hits.get(i).getScore(), 0.0);
            }
        } else {
            for (int i = 0; i < scoreDocs.length; i++) {
                Object[] luceneFields = ((FieldDoc) scoreDocs[i]).fields;
                WritableComparable[] chokFields = hits.get(i).getSortFields();
                assertEquals(luceneFields.length, chokFields.length);
                for (int j = 0; j < luceneFields.length; j++) {
                    assertEquals(luceneFields[j].toString(), chokFields[j].toString());
                }

                // Arrays.equals(scoreDocs, chokFields);
            }
        }
    }

}
