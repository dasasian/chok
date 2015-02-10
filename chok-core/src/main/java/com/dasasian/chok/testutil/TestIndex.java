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
package com.dasasian.chok.testutil;

import com.dasasian.chok.util.FileUtil;
import com.google.common.collect.ImmutableList;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Random;

/**
 * User: damith.chandrasekara
 * Date: 7/2/13
 */
public class TestIndex {
    private final int shardCount;
    private File indexFile;
    private String indexName;
    private String indexPath;
    private ImmutableList<File> shardFiles;

    public TestIndex(URL indexURL) {
        this.indexFile = new File(indexURL.getFile());
        indexName = indexFile.getName();
        indexPath = indexFile.getAbsolutePath();
        ImmutableList.Builder<File> builder = ImmutableList.builder();
        for (File shardFile : indexFile.listFiles(FileUtil.VISIBLE_FILES_FILTER)) {
            builder.add(shardFile);
        }
        shardFiles = builder.build();
        shardCount = shardFiles.size();
    }

    private TestIndex(TemporaryFolder temporaryFolder, int shardCount) {
        try {
            indexFile = temporaryFolder.newFolder();
            indexName = indexFile.getName();
            indexPath = indexFile.getAbsolutePath();
            ImmutableList.Builder<File> builder = ImmutableList.builder();
            for (int i = 0; i < shardCount; i++) {
                String shardName = "shard" + i;
                File shardFile = new File(indexFile, shardName);
                builder.add(shardFile);
            }
            shardFiles = builder.build();
            this.shardCount = shardFiles.size();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static TestIndex createTestIndex(TemporaryFolder temporaryFolder, int shardCount) {
        TestIndex testIndex = new TestIndex(temporaryFolder, shardCount);
        testIndex.createIndex();
        return testIndex;
    }

    public File getIndexFile() {
        return indexFile;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public int getShardCount() {
        return shardFiles.size();
    }

    public ImmutableList<File> getShardFiles() {
        return shardFiles;
    }

    public static final String DATA_FILE_NAME = "data.txt";

    private void createIndex() {
        createIndex(new String[]{"a", "b", "c"}, 2, 3);
    }

    private void createIndex(String[] wordList, int wordsPerDoc, int docsPerShard) {
        int count = wordList.length;
        Random random = new Random(System.currentTimeMillis());
        for (File indexShard : getShardFiles()) {
            if (!indexShard.mkdirs()) {
                throw new RuntimeException("Unable to create index dir " + indexShard.getAbsoluteFile());
            }
            File indexFile = new File(indexShard, DATA_FILE_NAME);
            try (PrintWriter bufferedWriter = new PrintWriter(new FileWriter(indexFile))) {
                // generate text first
                for (int i = 0; i < docsPerShard; i++) {
                    StringBuffer text = new StringBuffer();
                    for (int j = 0; j < wordsPerDoc; j++) {
                        text.append(wordList[random.nextInt(count)]);
                        text.append(" ");
                    }

                    bufferedWriter.println(text);
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Unable to write index", e);
            }
        }
    }


}