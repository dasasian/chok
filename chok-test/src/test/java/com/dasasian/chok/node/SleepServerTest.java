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
import com.dasasian.chok.testutil.NodeConfigurationFactory;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.server.sleep.SleepServer;
import org.slf4j.Logger;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Test for {@link SleepServer}.
 */
public class SleepServerTest extends AbstractTest {

    @SuppressWarnings("unused")
    private static Logger LOG = LoggerFactory.getLogger(SleepServerTest.class);
    private final NodeConfigurationFactory nodeConfigurationFactory = new TestNodeConfigurationFactory(temporaryFolder);

    @Test
    public void testNoSleep() throws Exception {
        SleepServer server = new SleepServer();
        long start = System.currentTimeMillis();
        server.sleep(0, 0, null);
        long time = System.currentTimeMillis() - start;
        assertTrue(time < 10);
    }

    @Test
    public void testSleep() throws Exception {
        SleepServer server = new SleepServer();
        long start = System.currentTimeMillis();
        server.sleep(100, 0, null);
        long time = System.currentTimeMillis() - start;
        assertTrue("took " + time, time >= 100);
    }

    @Test
    public void testVariation() throws IllegalArgumentException {
        SleepServer server = new SleepServer();
        long min = Integer.MAX_VALUE;
        long max = -1;
        for (int i = 0; i < 200; i++) {
            long n = checkTime(server);
            max = Math.max(n, max);
            min = Math.min(n, min);
        }
        assertTrue(max - min >= 9);
    }

    private long checkTime(SleepServer server) throws IllegalArgumentException {
        long start = System.currentTimeMillis();
        server.sleep(10, 5, null);
        return System.currentTimeMillis() - start;
    }

    @Test
    public void testShards() throws IllegalArgumentException {
        SleepServer server = new SleepServer();
        server.init("sleepy", nodeConfigurationFactory.getConfiguration());
        try {
            server.sleep(0L, 0, new String[]{"not-found"});
            fail("Should have failed");
        } catch (IllegalArgumentException e) {
            assertEquals("Node sleepy invalid shards: not-found", e.getMessage());
        }
        server.addShard("s1", null);
        server.sleep(0L, 0, new String[]{"s1"});
        try {
            server.sleep(0L, 0, new String[]{"s1", "s2"});
        } catch (IllegalArgumentException e) {
            assertEquals("Node sleepy invalid shards: s2", e.getMessage());
        }
        server.addShard("s2", null);
        server.sleep(0L, 0, new String[]{"s1", "s2"});
    }

}
