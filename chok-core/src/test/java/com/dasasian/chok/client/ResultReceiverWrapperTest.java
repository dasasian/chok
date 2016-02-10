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

import com.dasasian.chok.util.TestWatcherLoggingRule;
import com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Test for ResultReceiverWrapper
 * Created by damith.chandrasekara on 8/25/15.
 */
public class ResultReceiverWrapperTest {

    @Rule
    public TestRule watcher = new TestWatcherLoggingRule(ResultReceiverWrapper.class, "testDuplicateShards");

    @Test
    public void testCoverage() {
        @SuppressWarnings("unchecked")
        IResultReceiver<String> mockReceiver = mock(IResultReceiver.class);
        ResultReceiverWrapper<String> r = new ResultReceiverWrapper<>(ImmutableSet.of("a", "b", "c", "d", "e"), mockReceiver);
        assertEquals(0.0, r.getShardCoverage(), 0.000001);
        assertFalse(r.isComplete());
        r.addNodeResult("r1", ImmutableSet.of("a"));
        assertEquals(0.2, r.getShardCoverage(), 0.000001);
        assertFalse(r.isComplete());
        r.addNodeResult("r1", ImmutableSet.of("b"));
        assertEquals(0.4, r.getShardCoverage(), 0.000001);
        assertFalse(r.isComplete());
        r.addNodeResult("r1", ImmutableSet.of("c", "d"));
        assertEquals(0.8, r.getShardCoverage(), 0.000001);
        assertFalse(r.isComplete());
        r.addNodeResult("r1", ImmutableSet.of("e"));
        assertTrue(r.isComplete());
        assertEquals(1.0, r.getShardCoverage(), 0.000001);
    }

    @Test
    public void testDuplicateShards() {
        @SuppressWarnings("unchecked")
        IResultReceiver<String> mockReceiver = mock(IResultReceiver.class);
        ResultReceiverWrapper<String> r = new ResultReceiverWrapper<>(ImmutableSet.of("a", "b", "c"), mockReceiver);
        assertEquals(0.0, r.getShardCoverage(), 0.000001);
        verify(mockReceiver, never()).addResult(any(), any());
        assertFalse(r.isComplete());

        r.addNodeResult("foo", ImmutableSet.of("a", "b"));
        verify(mockReceiver, times(1)).addResult(any(), any());
        assertEquals(0.666, r.getShardCoverage(), 0.01);
        assertFalse(r.isComplete());

        r.addNodeResult("foo", ImmutableSet.of("b", "c"));
        verify(mockReceiver, times(2)).addResult(any(), any());
        assertEquals(1.0, r.getShardCoverage(), 0.000001);
        assertTrue(r.isComplete());

        r.addNodeResult("foo", ImmutableSet.of("c", "a"));
        verify(mockReceiver, times(3)).addResult(any(), any());
        assertEquals(1.0, r.getShardCoverage(), 0.000001);
        assertTrue(r.isComplete());
    }

    @Test
    public void testClosed() throws Exception {
        @SuppressWarnings("unchecked")
        IResultReceiver<String> mockReceiver = mock(IResultReceiver.class);
        ResultReceiverWrapper<String> r = new ResultReceiverWrapper<>(ImmutableSet.of("a", "b", "c"), mockReceiver);
        r.close();

        verify(mockReceiver, never()).addResult(any(), any());
        verify(mockReceiver, never()).addError(any(), any());

        r.addNodeResult("r1", ImmutableSet.of("a"));
        r.addNodeError(new Exception(), ImmutableSet.of("b", "c"));

        verify(mockReceiver, never()).addResult(any(), any());
        verify(mockReceiver, never()).addError(any(), any());
    }



}