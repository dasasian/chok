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

import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.util.TestLoggerWatcher;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Test for {@link ClientResult}.
 */
public class ClientResultTest extends AbstractTest {

    @Rule
    public TestLoggerWatcher resultsReceiverWrapperLoggingRule = TestLoggerWatcher.logErrors(ResultReceiverWrapper.class, "testResults", "testDuplicateResults", "testNoShards", "testUnknownShards", "testDuplicateErrors");

    @Test
    public void testToStringResults() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        assertEquals("ClientResult: 0 results, 0 errors, 0/3 shards", r.toString());
        r.addNodeResult("x", ImmutableSet.of("a"));
        assertEquals("ClientResult: 1 results, 0 errors, 1/3 shards", r.toString());
        r.addNodeResult("x", ImmutableSet.of("b"));
        assertEquals("ClientResult: 2 results, 0 errors, 2/3 shards", r.toString());
        r.addNodeResult(null, ImmutableSet.of("c"));
        assertEquals("ClientResult: 2 results, 0 errors, 3/3 shards (complete)", r.toString());
        assertEquals("ClientResult: 2 results, 0 errors, 3/3 shards (complete)", r.toString());
    }

    @Test
    public void testToStringErrors() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        assertEquals("ClientResult: 0 results, 0 errors, 0/3 shards", r.toString());
        r.addNodeError(new Throwable(""), ImmutableSet.of("a"));
        assertEquals("ClientResult: 0 results, 1 errors, 1/3 shards", r.toString());
        r.addNodeError(new Exception(""), ImmutableSet.of("b"));
        assertEquals("ClientResult: 0 results, 2 errors, 2/3 shards", r.toString());
        r.addNodeError(null, ImmutableSet.of("c"));
        assertEquals("ClientResult: 0 results, 2 errors, 3/3 shards (complete)", r.toString());
    }

    @Test
    public void testToStringMixed() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        assertEquals("ClientResult: 0 results, 0 errors, 0/3 shards", r.toString());
        r.addNodeResult("x", ImmutableSet.of("a"));
        assertEquals("ClientResult: 1 results, 0 errors, 1/3 shards", r.toString());
        r.addNodeResult("x", ImmutableSet.of("b"));
        assertEquals("ClientResult: 2 results, 0 errors, 2/3 shards", r.toString());
        r.addNodeError(new Exception(), ImmutableSet.of("c"));
        assertEquals("ClientResult: 2 results, 1 errors, 3/3 shards (complete)", r.toString());
    }

    @Test
    public void testResults() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c", "d"));
        assertEquals("[]", r.getResults().toString());
        r.addNodeResult("r1", ImmutableSet.of("a"));
        assertEquals("[r1]", r.getResults().toString());
        r.addNodeError(new Exception("foo"), ImmutableSet.of("b"));
        assertEquals("[r1]", r.getResults().toString());
        r.addNodeResult("r2", ImmutableSet.of("c"));
        assertEquals(8, r.getResults().toString().length());
        assertTrue(r.getResults().toString().indexOf("r1") > 0);
        assertTrue(r.getResults().toString().indexOf("r2") > 0);
        r.addNodeResult(null, ImmutableSet.of("c"));
        assertEquals(8, r.getResults().toString().length());
        assertTrue(r.getResults().toString().indexOf("r1") > 0);
        assertTrue(r.getResults().toString().indexOf("r2") > 0);
    }

    @Test
    public void testDuplicateResults() {
        ClientResult<Integer> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult(5, ImmutableSet.of("a"));
        r.addNodeResult(5, ImmutableSet.of("b"));
        r.addNodeResult(5, ImmutableSet.of("c"));
        assertEquals(3, r.getResults().size());
        assertEquals(3, r.entrySet().size());
        r.addNodeResult(5, ImmutableSet.of("a"));
        // todo is this the right behaviour?
        assertEquals(4, r.getResults().size());
        assertEquals(4, r.entrySet().size());
    }

    @Test
    public void testDuplicateErrors() {
        ClientResult<Integer> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        Throwable t = new Throwable();
        r.addNodeError(t, ImmutableSet.of("a"));
        r.addNodeError(t, ImmutableSet.of("b"));
        r.addNodeError(t, ImmutableSet.of("c"));
        assertEquals(3, r.getErrors().size());
        assertEquals(3, r.entrySet().size());
        r.addNodeError(t, ImmutableSet.of("a"));
        assertEquals(4, r.getErrors().size());
        assertEquals(4, r.entrySet().size());
    }

    @Test
    public void testErrors() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c", "d"));
        assertEquals("[]", r.getErrors().toString());
        assertNull(r.getError());
        assertFalse(r.isError());
        r.addNodeResult("r1", ImmutableSet.of("a"));
        assertEquals("[]", r.getErrors().toString());
        assertFalse(r.isError());
        assertNull(r.getError());
        r.addNodeError(new OutOfMemoryError("foo"), ImmutableSet.of("b"));
        assertEquals("[java.lang.OutOfMemoryError: foo]", r.getErrors().toString());
        assertEquals("java.lang.OutOfMemoryError: foo", r.getError().toString());
        assertTrue(r.isError());
        r.addNodeError(new NullPointerException(), ImmutableSet.of("c"));
        assertEquals(65, r.getErrors().toString().length());
        assertTrue(r.getErrors().toString().indexOf("java.lang.OutOfMemoryError: foo") > 0);
        assertTrue(r.getErrors().toString().indexOf("java.lang.NullPointerException") > 0);
        assertTrue(r.getError() instanceof OutOfMemoryError || r.getError() instanceof NullPointerException);
        assertTrue(r.isError());
    }

    @Test
    public void testNoShards() {
        try {
            new ClientResult<String>(ImmutableSet.of());
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            // Good.
        }
        try {
            new ClientResult<String>(ImmutableSet.of());
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            // Good.
        }
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult("foo", null);
        assertTrue(r.getSeenShards().isEmpty());
        assertTrue(r.getResults().isEmpty());
        r.addNodeResult("foo", Sets.newHashSet());
        assertTrue(r.getSeenShards().isEmpty());
        assertTrue(r.getResults().isEmpty());
        r.addNodeError(new Exception("foo"), null);
        assertTrue(r.getSeenShards().isEmpty());
        assertTrue(r.getErrors().isEmpty());
        r.addNodeError(new Exception("foo"), Sets.newHashSet());
        assertTrue(r.getSeenShards().isEmpty());
        assertTrue(r.getErrors().isEmpty());
    }

    @Test
    public void testNulls() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult(null, ImmutableSet.of("a"));
        assertEquals(1, r.getSeenShards().size());
        assertTrue(r.getSeenShards().contains("a"));
        assertTrue(r.getResults().isEmpty());
        assertTrue(r.getErrors().isEmpty());
        assertFalse(r.isError());
        assertEquals(1, r.getArrivalTimes().size());
        assertTrue(r.getArrivalTimes().get(0).toString().startsWith("null from [a] at "));
        sleep(3);
        r.addNodeError(null, ImmutableSet.of("b"));
        assertEquals(2, r.getSeenShards().size());
        assertTrue(r.getSeenShards().contains("b"));
        assertTrue(r.getResults().isEmpty());
        assertTrue(r.getErrors().isEmpty());
        assertFalse(r.isError());
        assertEquals(2, r.getArrivalTimes().size());
        assertTrue(r.getArrivalTimes().get(1).toString().startsWith("null from [b] at "));
        sleep(3);
        r.addNodeResult(null, ImmutableSet.of("c"));
        assertEquals(3, r.getSeenShards().size());
        assertTrue(r.getSeenShards().contains("c"));
        assertTrue(r.getResults().isEmpty());
        assertTrue(r.getErrors().isEmpty());
        assertFalse(r.isError());
        assertTrue(r.getArrivalTimes().get(2).toString().startsWith("null from [c] at "));
        assertEquals(3, r.getArrivalTimes().size());
        assertTrue(r.isComplete());
    }

    @Test
    public void testEntryBadResult() {
        ClientResult<ToStringFails> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult(new ToStringFails(), ImmutableSet.of("c"));
        assertTrue(r.entrySet().iterator().next().toString().startsWith("(toString() err) from [c] at "));
        r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult(null, ImmutableSet.of("c"));
        assertTrue(r.entrySet().iterator().next().toString().startsWith("null from [c] at "));
        r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeError(null, ImmutableSet.of("c"));
        assertTrue(r.entrySet().iterator().next().toString().startsWith("null from [c] at "));
    }

    @Test
    public void testMissingAndSeenShardsSingle() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        List<String> missing = new ArrayList<>(r.getMissingShards());
        Collections.sort(missing);
        assertEquals("[a, b, c]", missing.toString());
        assertTrue(r.getSeenShards().isEmpty());
        //
        r.addNodeResult("x", ImmutableSet.of("b"));
        missing = new ArrayList<>(r.getMissingShards());
        Collections.sort(missing);
        assertEquals("[a, c]", missing.toString());
        List<String> seen = new ArrayList<>(r.getSeenShards());
        Collections.sort(seen);
        assertEquals("[b]", seen.toString());
        //
        r.addNodeError(new Exception(""), ImmutableSet.of("a"));
        missing = new ArrayList<>(r.getMissingShards());
        Collections.sort(missing);
        assertEquals("[c]", missing.toString());
        seen = new ArrayList<>(r.getSeenShards());
        Collections.sort(seen);
        assertEquals("[a, b]", seen.toString());
        //
        r.addNodeResult("x", ImmutableSet.of("c"));
        assertTrue(r.getMissingShards().isEmpty());
        seen = new ArrayList<>(r.getSeenShards());
        Collections.sort(seen);
        assertEquals("[a, b, c]", seen.toString());
    }

    @Test
    public void testMissingAndSeenShardsMulti() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult("x", ImmutableSet.of("a", "b"));
        List<String> missing = new ArrayList<>(r.getMissingShards());
        Collections.sort(missing);
        assertEquals("[c]", missing.toString());
        List<String> seen = new ArrayList<>(r.getSeenShards());
        Collections.sort(seen);
        assertEquals("[a, b]", seen.toString());
        //
        r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult("x", ImmutableSet.of("a", "b", "c"));
        assertTrue(r.getMissingShards().isEmpty());
        missing = new ArrayList<>(r.getMissingShards());
        Collections.sort(missing);
        seen = new ArrayList<>(r.getSeenShards());
        Collections.sort(seen);
        assertEquals("[a, b, c]", seen.toString());
    }

    @Test
    public void testUnknownShards() {
        // This should not happen normally.
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        assertEquals(3, r.getMissingShards().size());
        assertEquals(0, r.getSeenShards().size());
        assertEquals(0, r.entrySet().size());
        assertFalse(r.isComplete());
        r.addNodeResult("foo", ImmutableSet.of("x"));
        assertEquals(3, r.getMissingShards().size());
        assertEquals(1, r.getSeenShards().size());
        assertEquals(1, r.entrySet().size());
        assertFalse(r.isComplete());
        r.addNodeResult("foo", ImmutableSet.of("y"));
        assertEquals(3, r.getMissingShards().size());
        assertEquals(2, r.getSeenShards().size());
        assertEquals(2, r.entrySet().size());
        assertFalse(r.isComplete());
        r.addNodeResult("foo", ImmutableSet.of("z"));
        assertEquals(3, r.getMissingShards().size());
        assertEquals(3, r.getSeenShards().size());
        assertEquals(3, r.entrySet().size());
        assertFalse(r.isComplete());
        r.addNodeResult("foo", ImmutableSet.of("a", "b", "c"));
        assertEquals(0, r.getMissingShards().size());
        assertEquals(6, r.getSeenShards().size());
        assertEquals(4, r.entrySet().size());
        assertTrue(r.isComplete());
    }

    @Test
    public void testArrivalTimes() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult("r1", ImmutableSet.of("a"));
        sleep(3);
        Throwable t = new Throwable();
        r.addNodeError(t, ImmutableSet.of("b"));
        sleep(3);
        r.addNodeResult("r2", ImmutableSet.of("c"));
        List<ClientResult<String>.Entry> entries = r.getArrivalTimes();
        assertNotNull(entries);
        assertEquals(3, entries.size());
        ClientResult<String>.Entry e1 = entries.get(0);
        ClientResult<String>.Entry e2 = entries.get(1);
        ClientResult<String>.Entry e3 = entries.get(2);
        assertEquals("r1", e1.result);
        assertEquals(t, e2.error);
        assertEquals("r2", e3.result);
        assertTrue(e2.time > e1.time);
        assertTrue(e3.time > e2.time);
    }

    @Test
    public void testArrivalTimesSorting() {
        String result = "foo";
        Throwable error = new Exception("bar");
        Set<String> shardA = new HashSet<>();
        shardA.add("a");
        Set<String> shardB = new HashSet<>();
        shardB.add("b");
        for (int i = 0; i < 10000; i++) {
            ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
            // todo something weird here (since the else block is never executed)
            if (i % 1 == 0) {
                r.addNodeResult(result, shardA);
                r.addNodeError(error, shardB);
            } else {
                r.addNodeResult(result, shardA);
                r.addNodeError(error, shardB);
            }
            List<ClientResult<String>.Entry> times = r.getArrivalTimes();
            assertEquals(2, times.size());
            assertEquals("foo", times.get(0).result);
            assertNull(times.get(0).error);
            assertEquals("[a]", times.get(0).shards.toString());
            assertNull(times.get(1).result);
            assertEquals("java.lang.Exception: bar", times.get(1).error.toString());
            assertEquals("[b]", times.get(1).shards.toString());
        }
    }

    @Test
    public void testMultithreaded() throws InterruptedException {
        ImmutableSet.Builder<String> shardBuilder = ImmutableSet.builder();
        final int size = 1000;
        for (int i = 0; i < size; i++) {
            shardBuilder.add("s" + i);
        }

        ImmutableSet<String> shards = shardBuilder.build();
        final ClientResult<Integer> clientResult = new ClientResult<>(shards);

        Random rand = new Random("testMultithreaded".hashCode());
        ExecutorService executor = Executors.newFixedThreadPool(15);
        int total = 0;
        for (int i = 0; i < size; i++) {
            final String shard = "s" + i;
            final int nodeResult = i;
            total += nodeResult;
            final long delay = rand.nextInt(50);
            executor.submit(() -> {
                sleep(delay);
                clientResult.addNodeResult(nodeResult, ImmutableSet.of(shard));
            });
        }
        executor.shutdown();
        executor.awaitTermination(3, TimeUnit.MINUTES);

        clientResult.close();

        assertEquals(String.format("ClientResult: %s results, 0 errors, %d/%d shards (closed) (complete)", size, size, size), clientResult.toString());
        int resultTotal = 0;
        for (ClientResult<Integer>.Entry e : clientResult) {
            resultTotal += e.result;
        }
        assertEquals(total, resultTotal);
    }

    @Test
    public void testIteratorEmpty() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        assertFalse(r.iterator().hasNext());
    }

    @Test
    public void testIteratorWhileAdding() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        r.addNodeResult("r1", ImmutableSet.of("a"));
        r.addNodeResult("r2", ImmutableSet.of("b"));
        Iterator<ClientResult<String>.Entry> i = r.iterator();
        r.addNodeResult("r3", ImmutableSet.of("c"));
        assertTrue(i.hasNext());
        ClientResult<String>.Entry e1 = i.next();
        assertTrue(e1.result.equals("r1") || e1.result.equals("r2"));
        assertTrue(e1.shards.size() == 1);
        assertTrue(e1.shards.iterator().next().equals("a") || e1.shards.iterator().next().equals("b"));
        ClientResult<String>.Entry e2 = i.next();
        assertTrue(e2.result.equals("r1") || e2.result.equals("r2"));
        assertTrue(e2.shards.size() == 1);
        assertTrue(e2.shards.iterator().next().equals("a") || e2.shards.iterator().next().equals("b"));
        assertFalse(e1.result.equals(e2.result));
        assertFalse(e1.shards.iterator().next().equals(e2.shards.iterator().next()));
        assertFalse(i.hasNext());
    }

    @Test
    public void testReadOnly() {
        ClientResult<String> r = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        checkReadOnly(r);
        r.addNodeResult("r1", ImmutableSet.of("a"));
        checkReadOnly(r);
        r.addNodeError(new Exception(), ImmutableSet.of("b"));
        checkReadOnly(r);
        r.addNodeResult("r3", ImmutableSet.of("c"));
        checkReadOnly(r);
    }

    private void checkReadOnly(ClientResult<String> r) {
        checkCollectionReadOnly(r.getAllShards());
        checkCollectionReadOnly(r.entrySet());
        checkCollectionReadOnly(r.getResults());
        checkCollectionReadOnly(r.getErrors());
        checkCollectionReadOnly(r.getSeenShards());
        try {
            r.iterator().remove();
            fail("Should be read only");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void checkCollectionReadOnly(Collection<T> s) {
        try {
            s.remove(0);
            fail("Should be read only");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            s.clear();
            fail("Should be read only");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        if (!s.isEmpty()) {
            try {
                s.remove(s.iterator().next());
                fail("Should be read only");
            } catch (UnsupportedOperationException e) {
                // expected
            }
        }
        try {
            s.removeAll(Lists.<T>newArrayList());
            fail("Should be read only");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            s.retainAll(new ArrayList<ClientResult<String>.Entry>());
            fail("Should be read only");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            s.add(null);
            fail("Should be read only");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            s.addAll(new ArrayList());
            fail("Should be read only");
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            s.iterator().remove();
            fail("Should be read only");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testStartTime() {
        ClientResult<String> r1 = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        sleep(10);
        ClientResult<String> r2 = new ClientResult<>(ImmutableSet.of("a", "b", "c"));
        assertTrue(r2.getStartTime() - r1.getStartTime() >= 10);
    }

    protected void sleep(long msec) {
        long now = System.currentTimeMillis();
        long waitUntil = now + msec;
        while (now < waitUntil) {
            long remainingTime = waitUntil - now;
            try {
                Thread.sleep(remainingTime);
            } catch (InterruptedException e) {
                // ignore and continue waiting
            }
            now = System.currentTimeMillis();
        }
    }

    protected static class ToStringFails {
        @Override
        public String toString() {
            throw new RuntimeException("err");
        }
    }

}
