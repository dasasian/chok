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
package com.dasasian.chok.client;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test for {@link ResultCompletePolicy}.
 */
public class ResultCompletePolicyTest {

    @Test
    public void testComplete() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        ResultCompletePolicy<String> rc = ResultCompletePolicy.awaitCompletion(60000);
        assertEquals("Wait up to 60000 ms for complete results.", rc.toString());
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 50000);
        r.addNodeResult("x", ImmutableSet.of("a"));
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 50000);
        r.addNodeResult("x", ImmutableSet.of("b"));
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 50000);
        r.addNodeResult("x", ImmutableSet.of("c"));
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) == 0);
        assertFalse(r.isClosed());
    }

    @Test
    public void testCoverageNoShutdown() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        ResultCompletePolicy<String> rc = ResultCompletePolicy.awaitCompletion(0, 60000, 0.5);
        assertEquals("Wait up to 0 ms for complete results, then 60000 ms for 0.5 coverage.", rc.toString());
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 50000);
        r.addNodeResult("x", ImmutableSet.of("a"));
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 50000);
        r.addNodeResult("x", ImmutableSet.of("b"));
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) == 0);
        assertFalse(r.isClosed());
        r.addNodeResult("x", ImmutableSet.of("c"));
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) == 0);
        assertFalse(r.isClosed());
    }

    @Test
    public void testCoverageShutdown() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        ResultCompletePolicy<String> rc = ResultCompletePolicy.awaitCompletion(0, 60000, 0.5);
        assertEquals("Wait up to 0 ms for complete results, then 60000 ms for 0.5 coverage.", rc.toString());
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 50000);
        r.addNodeResult("x", ImmutableSet.of("a"));
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 50000);
        r.addNodeResult("x", ImmutableSet.of("b"));
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) == 0);
        assertFalse(r.isClosed());
        r.addNodeResult("x", ImmutableSet.of("c"));
        assertTrue(rc.waitTime(r.getResultReceiverWrapper()) == 0);
        assertFalse(r.isClosed());
    }

    @Test
    public void testCompleteTiming() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        ResultCompletePolicy<String> rc = ResultCompletePolicy.awaitCompletion(1000);

        long now = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        long stop = start + 500;
        while (now < stop) {
            assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 400);
            sleep(1);
            now = System.currentTimeMillis();
        }
        stop = start + 1500;
        while (now < stop) {
            sleep(stop - now);
            now = System.currentTimeMillis();
        }
        stop = start + 2000;
        while (now < stop) {
            assertTrue(rc.waitTime(r.getResultReceiverWrapper()) == 0);
            sleep(1);
            now = System.currentTimeMillis();
        }
    }

    @Test
    public void testCoverageTiming1() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult("foo", ImmutableSet.of("a"));
        ResultCompletePolicy<String> rc = ResultCompletePolicy.awaitCompletion(500, 500, 0.5);
        assertEquals("Wait up to 500 ms for complete results, then 500 ms for 0.5 coverage.", rc.toString());
        long now = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        long stop = start + 800;
        while (now < stop) {
            // Waiting for complete. Then wait for coverage.
            assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 100);
            sleep(1);
            now = System.currentTimeMillis();
        }
        stop = start + 1500;
        while (now < stop) {
            sleep(stop - now);
            now = System.currentTimeMillis();
        }
        stop = start + 2000;
        while (now < stop) {
            // Expired.
            assertTrue(rc.waitTime(r.getResultReceiverWrapper()) == 0);
            sleep(1);
            now = System.currentTimeMillis();
        }
    }

    @Test
    public void testCoverageTiming2() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult("foo", ImmutableSet.of("a", "b"));
        ResultCompletePolicy<String> rc = ResultCompletePolicy.awaitCompletion(500, 500, 0.5);
        assertEquals("Wait up to 500 ms for complete results, then 500 ms for 0.5 coverage.", rc.toString());
        long now = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        long stop = start + 250;
        while (now < stop) {
            // Wait for complete.
            assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 300);
            sleep(1);
            now = System.currentTimeMillis();
        }
        stop = start + 600;
        while (now < stop) {
            sleep(stop - now);
            now = System.currentTimeMillis();
        }
        stop = start + 1200;
        while (now < stop) {
            // Coverage is good enough.
            assertTrue(rc.waitTime(r.getResultReceiverWrapper()) == 0);
            sleep(1);
            now = System.currentTimeMillis();
        }
    }

    @Test
    public void testCoverage() {
        ImmutableSet.Builder<String> shardsBuilder = ImmutableSet.builder();
        for (int i = 0; i < 1000; i++) {
            shardsBuilder.add("s" + i);
        }
        ClientResult<String> r = new ClientResult<>(shardsBuilder.build());

        ResultCompletePolicy<String> rc = ResultCompletePolicy.awaitCompletion(0, 60000, (879.0 / 1000.0));
        for (int i = 0; i < 1000; i++) {
            try {
                r.addNodeResult("foo", ImmutableSet.of("s" + i));
                if (i < 878) {
                    assertTrue(rc.waitTime(r.getResultReceiverWrapper()) > 30000);
                } else {
                    assertTrue(rc.waitTime(r.getResultReceiverWrapper()) == 0);
                }
            } catch (Error e) {
                System.err.println("i = " + i);
                throw e;
            }
        }
    }

    private void sleep(long msec) {
        long now = System.currentTimeMillis();
        long stop = now + msec;
        while (now < stop) {
            try {
                Thread.sleep(stop - now);
            } catch (InterruptedException e) {
                // proceed
            }
            now = System.currentTimeMillis();
        }
    }

}
