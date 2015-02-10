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
package com.dasasian.chok.lucene.testutil;

import com.dasasian.chok.lucene.LuceneServer;
import com.dasasian.chok.testutil.IndexGenerator;
import com.dasasian.chok.testutil.TestIndex;
import com.google.common.base.Splitter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Random;

/**
 * User: damith.chandrasekara
 * Date: 7/2/13
 */
public class LuceneIndexGenerator implements IndexGenerator<LuceneServer> {

    public static final LuceneIndexGenerator SINGLETON = new LuceneIndexGenerator();
    public static final Splitter SPACE_SPLITTER = Splitter.on(" ").omitEmptyStrings();

    public static TestIndex createTestIndex(TemporaryFolder temporaryFolder, int indexShards) {
        TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, indexShards);
        SINGLETON.createIndex(testIndex);
        return testIndex;
    }

    public static LuceneIndexGenerator create() {
        return new LuceneIndexGenerator();
    }

    @Override
    public void createIndex(TestIndex testIndex) {
        createIndex(testIndex, new String[]{"a", "b", "c"}, 3, 10);
    }

    public void createIndex(TestIndex testIndex, String[] wordList, int wordsPerDoc, int docsPerShard) {
        long startTime = System.currentTimeMillis();
        try {
            for (File index : testIndex.getShardFiles()) {
                int count = wordList.length;
                Random random = new Random(System.currentTimeMillis());
                IndexWriter indexWriter = new IndexWriter(FSDirectory.open(index), new StandardAnalyzer(Version.LUCENE_30), true, IndexWriter.MaxFieldLength.UNLIMITED);
                for (int i = 0; i < docsPerShard; i++) {
                    // generate text first
                    StringBuilder text = new StringBuilder();
                    for (int j = 0; j < wordsPerDoc; j++) {
                        text.append(wordList[random.nextInt(count)]);
                        text.append(" ");
                    }

                    Document document = new Document();
                    document.add(new Field("key", "key_" + i, Field.Store.NO, Field.Index.NOT_ANALYZED));
                    document.add(new Field("text", text.toString(), Field.Store.NO, Field.Index.ANALYZED));
                    indexWriter.addDocument(document);

                }
                indexWriter.optimize();
                indexWriter.close();
                System.out.println("Index created with : " + docsPerShard + " documents in " + (System.currentTimeMillis() - startTime) + " ms");

                // when we are ready we move the index to the final destination and write
                // a done flag file we can use in shell scripts to identify the move is
                // done.

                new File(index, "done").createNewFile();
            }

        }
        catch (Exception e) {
            throw new RuntimeException("Unable to write index", e);
        }
    }

    /**
     * creates a dictionary of words based on the input text.
     */
    //    private String[] getWordList(String input) {
    //        try {
    //            Set<String> hashSet = ImmutableSet.copyOf(Iterables.concat(Iterables.transform(Files.readLines(new File(input), Charset.defaultCharset()), new Function<String, Iterable<String>>() {
    //                @Override
    //                public Iterable<String> apply(@NonNull java.lang.String input) {
    //                    return SPACE_SPLITTER.split(input);
    //                }
    //            })));
    //            return Iterables.toArray(hashSet, String.class);
    //        }
    //        catch (IOException e) {
    //            throw new RuntimeException("Unable to read sample text", e);
    //        }
    //    }

}
