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
package com.dasasian.chok.node;

import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.ThrottledInputStream.ThrottleSemaphore;
import com.dasasian.chok.util.UtilModule;
import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ShardManagerTest extends AbstractTest {

    protected static Injector injector = Guice.createInjector(new UtilModule());

    protected final TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 4);
    private final File testFile = testIndex.getIndexFile();

    public void setUp() {
        // _managerFolder = temporaryFolder.newFolder("managerFolder");
        assertTrue("test file" + testFile + " does not exists", testFile.exists());
    }

    @Test
    public void testThrottling() throws Exception {
        String shardName = "shard";
        Path managerFolder = temporaryFolder.newFolder().toPath();

        // measure transfer rate with no throttle
        ShardManager shardManager = new ShardManager(managerFolder, injector.getInstance(ChokFileSystem.Factory.class), null);
        long startTime = System.currentTimeMillis();
        long fileLength = FileUtils.sizeOf(testFile);
        for (int i = 0; i < 10; i++) {
            shardManager.installShard(shardName + i, testFile.toURI(), false);
        }
        long durationInMilliSec = System.currentTimeMillis() - startTime;
        long bytesPerSec = (fileLength * 10) / durationInMilliSec * 1000;
        printResults(fileLength, durationInMilliSec, bytesPerSec);

        // now do the same throttled to half speed
        for (int i = 0; i < 10; i++) {
            shardManager.uninstallShard(shardName + i);
        }
        shardManager = new ShardManager(managerFolder, injector.getInstance(ChokFileSystem.Factory.class), new ThrottleSemaphore(Float.max(1, bytesPerSec / 3)));
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            shardManager.installShard(shardName + i, testFile.toURI(), false);
        }
        durationInMilliSec = (System.currentTimeMillis() - startTime);
        long bytesPerSec2 = (fileLength * 10) / durationInMilliSec * 1000;
        printResults(fileLength, durationInMilliSec, bytesPerSec2);
        assertThat(bytesPerSec2, almostEquals(bytesPerSec / 3, 300000));
    }

    private void printResults(long fileLength, long durationInSec, long bytesPerSec) {
        System.out.println("took " + durationInSec + " ms to install ~" + fileLength / 1024 / 1024 + " MB");
        System.out.println("rate " + bytesPerSec + " bytes/sec");
    }

    @Test
    public void testGetLocalShardFolders() throws IOException {
        Path managerFolder = temporaryFolder.newFolder().toPath();

        // measure transfer rate with no throttle
        ShardManager shardManager = new ShardManager(managerFolder, injector.getInstance(ChokFileSystem.Factory.class), null);

        Iterable<Path> paths = shardManager.getLocalShardFolders(null);
        assertTrue(Iterables.isEmpty(paths));

        Path shard1Folder = managerFolder.resolve("index1#shard1");
        Files.createDirectories(shard1Folder);
        paths = shardManager.getLocalShardFolders(null);
        assertEquals(1, Iterables.size(paths));

        Path shard2Folder = managerFolder.resolve("index1#shard2@12345");
        Files.createDirectories(shard2Folder);
        paths = shardManager.getLocalShardFolders(null);
        assertEquals(2, Iterables.size(paths));

        paths = shardManager.getLocalShardFolders("index1#shard1");
        assertEquals(1, Iterables.size(paths));

        paths = shardManager.getLocalShardFolders("index1#shard2");
        assertEquals(1, Iterables.size(paths));

    }

    @Test
    public void testGetLastModified() throws IOException {
        Path managerFolder = temporaryFolder.newFolder().toPath();

        // measure transfer rate with no throttle
        ShardManager shardManager = new ShardManager(managerFolder, injector.getInstance(ChokFileSystem.Factory.class), null);

        assertEquals(0L, shardManager.getLastModified("junit2527890647690981987#shard0"));
        assertEquals(1438468838000L, shardManager.getLastModified("junit2527890647690981987#shard0@1438468838000"));
    }
}
