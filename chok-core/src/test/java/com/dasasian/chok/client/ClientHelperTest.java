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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.fest.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.AdditionalMatchers;
import org.mockito.Matchers;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

/**
 * Tests for ClientHelper
 *
 * Created by damith.chandrasekara on 8/26/15.
 */
public class ClientHelperTest {

    private SetMultimap<String, String> indexToShards;
    private INodeProxyManager proxyManager;
    private INodeSelectionPolicy selectionPolicy;
    private ClientHelper clientHelper;
    private ITestServer testServer;
    private ExecutorService executorService;

    @Rule
    public TestRule watcher = new TestWatcherLoggingRule(NodeInteraction.class,
            "testBroadcastInternalReceiver",
            "testBroadcastInternalReceiverWrapper",
            "testBroadcastInternal");

    @Before
    public void setUp() throws Exception {
        indexToShards = HashMultimap.create();

        testServer = mock(ITestServer.class);
        proxyManager = mock(INodeProxyManager.class);
        when(proxyManager.getProxy(Matchers.anyString(), Matchers.anyBoolean())).thenReturn(testServer);

        selectionPolicy = new DefaultNodeSelectionPolicy();
        executorService = Executors.newCachedThreadPool();
        clientHelper = new ClientHelper(executorService, 1, indexToShards, proxyManager, selectionPolicy);
    }

    @After
    public void tearDown() throws Exception {
        indexToShards = null;
        testServer = null;
        proxyManager = null;
        selectionPolicy = null;
        clientHelper = null;
        executorService.shutdown();
    }

    @Test
    public void testBroadcastInternalReceiver() throws Exception {
        Method method = ITestServer.class.getMethod("testMethod", String.class, String[].class);
        Object[] args = new Object[]{"question", null};

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of("node1", "node1#shard1");

            doReturn(null).when(testServer).testMethod(anyString(), any(String[].class));

            @SuppressWarnings("unchecked")
            IResultReceiver<String> resultReceiver = mock(IResultReceiver.class);

            clientHelper.broadcastInternalReceiver(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, resultReceiver, args);

            verify(resultReceiver, never()).addError(any(Throwable.class), any());
            verify(resultReceiver, times(1)).addResult(anyString(), eq(ImmutableSet.of("node1#shard1")));
        }

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of("node1", "node1#shard1");

            doReturn("answer").when(testServer).testMethod(anyString(), any(String[].class));

            @SuppressWarnings("unchecked")
            IResultReceiver<String> resultReceiver = mock(IResultReceiver.class);

            clientHelper.broadcastInternalReceiver(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, resultReceiver, args);

            verify(resultReceiver, never()).addError(any(Throwable.class), any());
            verify(resultReceiver, times(1)).addResult(eq("answer"), eq(ImmutableSet.of("node1#shard1")));
        }

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of("node1", "node1#shard1");

            doThrow(new RuntimeException("test exception")).when(testServer).testMethod(anyString(), any(String[].class));

            @SuppressWarnings("unchecked")
            IResultReceiver<String> resultReceiver = mock(IResultReceiver.class);

            clientHelper.broadcastInternalReceiver(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, resultReceiver, args);

            verify(resultReceiver, times(1)).addError(any(Throwable.class), eq(ImmutableSet.of("node1#shard1")));
            verify(resultReceiver, never()).addResult(eq("answer"), any());
        }

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of(
                    "node1", "node1#shard1",
                    "node2", "node2#shard2");

            doReturn("answer").when(testServer).testMethod(anyString(), AdditionalMatchers.aryEq(Arrays.array("node1#shard1")));
            doThrow(new RuntimeException("test exception")).when(testServer).testMethod(anyString(), AdditionalMatchers.aryEq(Arrays.array("node2#shard2")));

            @SuppressWarnings("unchecked")
            IResultReceiver<String> resultReceiver = mock(IResultReceiver.class);

            clientHelper.broadcastInternalReceiver(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, resultReceiver, args);

            verify(resultReceiver, times(1)).addError(any(Throwable.class), eq(ImmutableSet.of("node2#shard2")));
            verify(resultReceiver, times(1)).addResult(eq("answer"), eq(ImmutableSet.of("node1#shard1")));
        }

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of(
                    "node1", "node1#shard1",
                    "node2", "node2#shard2");

            doReturn("answer").when(testServer).testMethod(anyString(), AdditionalMatchers.aryEq(Arrays.array("node1#shard1")));
            doReturn("answer2").when(testServer).testMethod(anyString(), AdditionalMatchers.aryEq(Arrays.array("node2#shard2")));

            @SuppressWarnings("unchecked")
            IResultReceiver<String> resultReceiver = mock(IResultReceiver.class);

            clientHelper.broadcastInternalReceiver(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, resultReceiver, args);

            verify(resultReceiver, times(2)).addResult(anyString(), any());
            verify(resultReceiver, times(1)).addResult(eq("answer"), eq(ImmutableSet.of("node1#shard1")));
            verify(resultReceiver, times(1)).addResult(eq("answer2"), eq(ImmutableSet.of("node2#shard2")));
        }
    }

    @Test
    public void testBroadcastInternalReceiverWrapper() throws Exception {
        Method method = ITestServer.class.getMethod("testMethod", String.class, String[].class);
        Object[] args = new Object[]{"question", null};

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of("node1", "node1#shard1");

            doReturn(null).when(testServer).testMethod(anyString(), any(String[].class));

            @SuppressWarnings("unchecked")
            IResultReceiver<String> resultReceiver = mock(IResultReceiver.class);

            @SuppressWarnings("unchecked")
            IResultReceiverWrapper<String> resultReceiverWrapper = new ResultReceiverWrapper(ImmutableSet.copyOf(node2Shard.values()), resultReceiver);

            clientHelper.broadcastInternalReceiverWrapper(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, resultReceiverWrapper, args);

            assertThat(resultReceiverWrapper.getShardCoverage(), is(equalTo(1.0)));
            assertThat(resultReceiverWrapper.getSeenShards(), hasItems("node1#shard1"));
            assertThat(resultReceiverWrapper.isClosed(), is(false));
            assertThat(resultReceiverWrapper.isComplete(), is(true));

            verify(resultReceiver, never()).addError(any(Throwable.class), any());
            verify(resultReceiver, times(1)).addResult(anyString(), eq(ImmutableSet.of("node1#shard1")));
        }

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of("node1", "node1#shard1");

            doReturn("answer").when(testServer).testMethod(anyString(), any(String[].class));

            @SuppressWarnings("unchecked")
            IResultReceiver<String> resultReceiver = mock(IResultReceiver.class);

            @SuppressWarnings("unchecked")
            IResultReceiverWrapper<String> resultReceiverWrapper = new ResultReceiverWrapper(ImmutableSet.copyOf(node2Shard.values()), resultReceiver);

            clientHelper.broadcastInternalReceiverWrapper(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, resultReceiverWrapper, args);

            assertThat(resultReceiverWrapper.getShardCoverage(), is(equalTo(1.0)));
            assertThat(resultReceiverWrapper.getSeenShards(), hasItems("node1#shard1"));
            assertThat(resultReceiverWrapper.isClosed(), is(false));
            assertThat(resultReceiverWrapper.isComplete(), is(true));

            verify(resultReceiver, never()).addError(any(Throwable.class), any());
            verify(resultReceiver, times(1)).addResult(eq("answer"), eq(ImmutableSet.of("node1#shard1")));
        }

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of("node1", "node1#shard1");

            doThrow(new RuntimeException("test exception")).when(testServer).testMethod(anyString(), any(String[].class));

            @SuppressWarnings("unchecked")
            IResultReceiver<String> resultReceiver = mock(IResultReceiver.class);

            @SuppressWarnings("unchecked")
            IResultReceiverWrapper<String> resultReceiverWrapper = new ResultReceiverWrapper(ImmutableSet.copyOf(node2Shard.values()), resultReceiver);

            clientHelper.broadcastInternalReceiverWrapper(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, resultReceiverWrapper, args);

            assertThat(resultReceiverWrapper.getShardCoverage(), is(equalTo(1.0)));
            assertThat(resultReceiverWrapper.getSeenShards(), hasItems("node1#shard1"));
            assertThat(resultReceiverWrapper.isClosed(), is(false));
            assertThat(resultReceiverWrapper.isComplete(), is(true));

            verify(resultReceiver, times(1)).addError(any(Throwable.class), eq(ImmutableSet.of("node1#shard1")));
            verify(resultReceiver, never()).addResult(eq("answer"), any());
        }

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of(
                    "node1", "node1#shard1",
                    "node2", "node2#shard2");

            doReturn("answer").when(testServer).testMethod(anyString(), AdditionalMatchers.aryEq(Arrays.array("node1#shard1")));
            doThrow(new RuntimeException("test exception")).when(testServer).testMethod(anyString(), AdditionalMatchers.aryEq(Arrays.array("node2#shard2")));

            @SuppressWarnings("unchecked")
            IResultReceiver<String> resultReceiver = mock(IResultReceiver.class);

            @SuppressWarnings("unchecked")
            IResultReceiverWrapper<String> resultReceiverWrapper = new ResultReceiverWrapper(ImmutableSet.copyOf(node2Shard.values()), resultReceiver);

            clientHelper.broadcastInternalReceiverWrapper(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, resultReceiverWrapper, args);

            assertThat(resultReceiverWrapper.getShardCoverage(), is(equalTo(1.0)));
            assertThat(resultReceiverWrapper.getSeenShards(), hasItems("node1#shard1", "node2#shard2"));
            assertThat(resultReceiverWrapper.isClosed(), is(false));
            assertThat(resultReceiverWrapper.isComplete(), is(true));

            verify(resultReceiver, times(1)).addError(any(Throwable.class), eq(ImmutableSet.of("node2#shard2")));
            verify(resultReceiver, times(1)).addResult(eq("answer"), eq(ImmutableSet.of("node1#shard1")));
        }
    }

    @Test
    public void testBroadcastInternal() throws Exception {
        Method method = ITestServer.class.getMethod("testMethod", String.class, String[].class);
        Object[] args = new Object[]{"question", null};

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of("node1", "node1#shard1");

            doReturn(null).when(testServer).testMethod(anyString(), any(String[].class));

            ClientResult<String> result = clientHelper.broadcastInternal(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, args);

            assertThat(result.getShardCoverage(), is(equalTo(1.0)));
            assertThat(result.getSeenShards(), hasItems("node1#shard1"));
            assertThat(result.isClosed(), is(true));
            assertThat(result.isComplete(), is(true));

            // should have 0 results when null is returned
            assertThat(result.getErrors().size(), is(equalTo(0)));
            assertThat(result.getResults().size(), is(equalTo(0)));
        }

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of("node1", "node1#shard1");

            doReturn("answer").when(testServer).testMethod(anyString(), any(String[].class));

            ClientResult<String> result = clientHelper.broadcastInternal(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, args);

            assertThat(result.getShardCoverage(), is(equalTo(1.0)));
            assertThat(result.getSeenShards(), hasItems("node1#shard1"));
            assertThat(result.isClosed(), is(true));
            assertThat(result.isComplete(), is(true));

            // should have 1 result
            assertThat(result.getErrors().size(), is(equalTo(0)));
            assertThat(result.getResults().size(), is(equalTo(1)));
            assertThat(result.getResults(), hasItems("answer"));
        }

        {
            ImmutableSetMultimap<String, String> node2Shard = ImmutableSetMultimap.of("node1", "node1#shard1");

            doThrow(new RuntimeException("test exception")).when(testServer).testMethod(anyString(), any(String[].class));

            ClientResult<String> result = clientHelper.broadcastInternal(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shard, args);

            assertThat(result.getShardCoverage(), is(equalTo(1.0)));
            assertThat(result.getSeenShards(), hasItems("node1#shard1"));
            assertThat(result.isClosed(), is(true));
            assertThat(result.isComplete(), is(true));

            // should have 1 result
            assertThat(result.getErrors().size(), is(equalTo(1)));
            assertThat(result.getResults().size(), is(equalTo(0)));
        }

        {
            ImmutableSetMultimap<String, String> node2Shards = ImmutableSetMultimap.of(
                    "node1", "node1#shard1",
                    "node2", "node2#shard2",
                    "node2", "node2#shard3");

            doReturn("answer").when(testServer).testMethod(anyString(), any(String[].class));

            ClientResult<String> result = clientHelper.broadcastInternal(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shards, args);

            assertThat(result.getShardCoverage(), is(equalTo(1.0)));
            assertThat(result.getSeenShards(), hasItems("node1#shard1", "node2#shard2", "node2#shard3"));
            assertThat(result.isClosed(), is(true));
            assertThat(result.isComplete(), is(true));

            assertThat(result.getErrors().size(), is(equalTo(0)));
            // should only get 2 since we are only querying 2 nodes
            assertThat(result.getResults().size(), is(equalTo(2)));
            assertThat(result.getResults(), hasItems("answer", "answer"));
        }

        {
            ImmutableSetMultimap<String, String> node2Shards = ImmutableSetMultimap.of(
                    "node1", "node1#shard1",
                    "node2", "node2#shard2",
                    "node3", "node3#shard3");

            doReturn("answer").when(testServer).testMethod(anyString(), any(String[].class));

            ClientResult<String> result = clientHelper.broadcastInternal(ResultCompletePolicy.awaitCompletion(1000),
                    method, 1, node2Shards, args);

            assertThat(result.getShardCoverage(), is(equalTo(1.0)));
            assertThat(result.getSeenShards(), hasItems("node1#shard1", "node2#shard2", "node3#shard3"));
            assertThat(result.isClosed(), is(true));
            assertThat(result.isComplete(), is(true));
            assertThat(result.getErrors().size(), is(equalTo(0)));
            // should only get 2 since we are only querying 2 nodes
            assertThat(result.getResults().size(), is(equalTo(3)));
            assertThat(result.getResults(), hasItems("answer", "answer", "answer"));
        }
    }

    public interface ITestServer extends VersionedProtocol {
        String testMethod(String param, String[] shards);
    }

}