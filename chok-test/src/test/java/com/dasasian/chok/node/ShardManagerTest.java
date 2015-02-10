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
import com.dasasian.chok.util.ThrottledInputStream.ThrottleSemaphore;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ShardManagerTest extends AbstractTest {

    protected final TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 4);
    private final File _testFile = testIndex.getIndexFile();

    public void setUp() {
        // _managerFolder = temporaryFolder.newFolder("managerFolder");
        assertTrue("test file" + _testFile + " does not exists", _testFile.exists());
    }

    @Test
    public void testThrottling() throws Exception {
        String shardName = "shard";
        File managerFolder = temporaryFolder.newFolder("managerFolder");

        // measure transfer rate with no throttle
        ShardManager shardManager = new ShardManager(managerFolder);
        long startTime = System.currentTimeMillis();
        long fileLength = org.apache.hadoop.fs.FileUtil.getDU(_testFile);
        for (int i = 0; i < 10; i++) {
            shardManager.installShard(shardName + i, _testFile.getAbsolutePath());
        }
        long durationInMilliSec = System.currentTimeMillis() - startTime;
        long bytesPerSec = (fileLength * 10) / durationInMilliSec * 1000;
        printResults(fileLength, durationInMilliSec, bytesPerSec);

        // now do the same throttled to half speed
        for (int i = 0; i < 10; i++) {
            shardManager.uninstallShard(shardName + i);
        }
        shardManager = new ShardManager(managerFolder, new ThrottleSemaphore(bytesPerSec / 3));
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            shardManager.installShard(shardName + i, _testFile.getAbsolutePath());
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
}
