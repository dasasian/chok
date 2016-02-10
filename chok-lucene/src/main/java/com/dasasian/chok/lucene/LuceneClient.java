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
package com.dasasian.chok.lucene;

import com.dasasian.chok.client.Client;
import com.dasasian.chok.client.ClientResult;
import com.dasasian.chok.client.INodeSelectionPolicy;
import com.dasasian.chok.client.ResultCompletePolicy;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.ChokException;
import com.dasasian.chok.util.ClientConfiguration;
import com.dasasian.chok.util.ZkConfiguration;
import org.apache.hadoop.io.MapWritable;
import org.slf4j.Logger;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * Default implementation of {@link ILuceneClient}.
 */
public class LuceneClient implements ILuceneClient {

    protected final static Logger LOG = LoggerFactory.getLogger(LuceneClient.class);
    private static final Method SEARCH_METHOD;
    private static final Method SORTED_SEARCH_METHOD;
    private static final Method FILTERED_SEARCH_METHOD;
    private static final Method FILTERED_SORTED_SEARCH_METHOD;
    private static final int SEARCH_METHOD_SHARD_ARG_IDX = 2;
    static {
        try {
            SEARCH_METHOD = ILuceneServer.class.getMethod("search", QueryWritable.class, DocumentFrequencyWritable.class, String[].class, Long.TYPE, Integer.TYPE);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method search() in ILuceneSearch!");
        }
        try {
            SORTED_SEARCH_METHOD = ILuceneServer.class.getMethod("search", QueryWritable.class, DocumentFrequencyWritable.class, String[].class, Long.TYPE, Integer.TYPE, SortWritable.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method search() in ILuceneSearch!");
        }
        try {
            FILTERED_SEARCH_METHOD = ILuceneServer.class.getMethod("search", QueryWritable.class, DocumentFrequencyWritable.class, String[].class, Long.TYPE, Integer.TYPE, FilterWritable.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method search() in ILuceneSearch!");
        }
        try {
            FILTERED_SORTED_SEARCH_METHOD = ILuceneServer.class.getMethod("search", QueryWritable.class, DocumentFrequencyWritable.class, String[].class, Long.TYPE, Integer.TYPE, SortWritable.class, FilterWritable.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method search() in ILuceneSearch!");
        }
    }
    private static final Method COUNT_METHOD;
    private static final Method FILTER_COUNT_METHOD;
    private static final int COUNT_METHOD_SHARD_ARG_IDX = 1;
    private static final int FILTER_COUNT_METHOD_SHARD_ARG_IDX = 2;
    static {
        try {
            COUNT_METHOD = ILuceneServer.class.getMethod("getResultCount", QueryWritable.class, String[].class, Long.TYPE);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method getResultCount() in ILuceneSearch!");
        }
        try {
            FILTER_COUNT_METHOD = ILuceneServer.class.getMethod("getResultCount", QueryWritable.class, FilterWritable.class, String[].class, Long.TYPE);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method getResultCount() in ILuceneSearch!");
        }
    }
    private static final Method DOC_FREQ_METHOD;
    private static final int DOC_FREQ_METHOD_SHARD_ARG_IDX = 1;
    static {
        try {
            DOC_FREQ_METHOD = ILuceneServer.class.getMethod("getDocFreqs", QueryWritable.class, String[].class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method getDocFreqs() in ILuceneSearch!");
        }
    }
    /*
     * public MapWritable getDetails(String[] shards, int docId, String[] fields)
     * throws IOException; public MapWritable getDetails(String[] shards, int
     * docId) throws IOException;
     */
    private static final Method GET_DETAILS_METHOD;
    private static final Method GET_DETAILS_FIELDS_METHOD;
    private static final int GET_DETAILS_METHOD_SHARD_ARG_IDX = 0;
    private static final int GET_DETAILS_FIELDS_METHOD_SHARD_ARG_IDX = 0;
    static {
        try {
            GET_DETAILS_METHOD = ILuceneServer.class.getMethod("getDetails", String[].class, Integer.TYPE);
            GET_DETAILS_FIELDS_METHOD = ILuceneServer.class.getMethod("getDetails", String[].class, Integer.TYPE, String[].class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method getDetails() in ILuceneSearch!");
        }
    }
    private long timeout = 12000;
    private Client client;

    public LuceneClient() {
        client = new Client(getServerClass());
    }

    public LuceneClient(final INodeSelectionPolicy nodeSelectionPolicy) {
        client = new Client(getServerClass(), nodeSelectionPolicy);
    }
    public LuceneClient(InteractionProtocol protocol) {
        client = new Client(getServerClass(), protocol);
    }
    public LuceneClient(final ZkConfiguration zkConfig) {
        client = new Client(getServerClass(), zkConfig);
    }
    public LuceneClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig) {
        client = new Client(getServerClass(), policy, zkConfig);
    }

    public LuceneClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig, ClientConfiguration clientConfiguration) {
        client = new Client(getServerClass(), policy, zkConfig, clientConfiguration);
    }

    @SuppressWarnings("unused")
    private static Method getMethod(String name, Class<?>... parameterTypes) {
        try {
            return ILuceneServer.class.getMethod("search", parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method " + name + "(" + Arrays.asList(parameterTypes) + ") in ILuceneSearch!");
        }
    }

    public Client getClient() {
        return client;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public Hits search(final Query query, final String[] indexNames) throws ChokException {
        return search(query, indexNames, Integer.MAX_VALUE);
    }

    @Override
    public Hits search(final Query query, final String[] indexNames, final int count) throws ChokException {
        return search(query, indexNames, count, null, null);
    }

    @Override
    public Hits search(final Query query, final String[] indexNames, final int count, final Sort sort) throws ChokException {
        return search(query, indexNames, count, sort, null);
    }

    @Override
    public Hits search(final Query query, final String[] indexNames, final int count, final Sort sort, final Filter filter) throws ChokException {
        final DocumentFrequencyWritable docFreqs = getDocFrequencies(query, indexNames);
        ClientResult<HitsMapWritable> results;

        if (sort == null && filter == null) {
            results = client.broadcastToIndices(ResultCompletePolicy.awaitCompletion(timeout), SEARCH_METHOD, SEARCH_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), docFreqs, null, timeout, count);
        } else if (sort != null && filter == null) {
            results = client.broadcastToIndices(ResultCompletePolicy.awaitCompletion(timeout), SORTED_SEARCH_METHOD, SEARCH_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), docFreqs, null, timeout, count, new SortWritable(sort));
        } else if (sort == null && filter != null) {
            results = client.broadcastToIndices(ResultCompletePolicy.awaitCompletion(timeout), FILTERED_SEARCH_METHOD, SEARCH_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), docFreqs, null, timeout, count, new FilterWritable(filter));
        } else {
            results = client.broadcastToIndices(ResultCompletePolicy.awaitCompletion(timeout), FILTERED_SORTED_SEARCH_METHOD, SEARCH_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), docFreqs, null, timeout, count, new SortWritable(sort), new FilterWritable(filter));
        }
        if (results.isError()) {
            throw results.getChokException();
        }
        Hits result = new Hits();
        HitsMapWritable exampleHitWritable = null;
        if (!results.getMissingShards().isEmpty()) {
            LOG.warn("incomplete result - missing shard-results: " + results.getMissingShards() + ", " + results.getShardCoverage());
        }
        for (HitsMapWritable hmw : results.getResults()) {
            List<Hit> hits = hmw.getHitList();
            if (exampleHitWritable == null && !hits.isEmpty()) {
                exampleHitWritable = hmw;
            }
            result.addTotalHits(hmw.getTotalHits());
            result.addHits(hits);
        }
        long start = 0;
        if (LOG.isDebugEnabled()) {
            start = System.currentTimeMillis();
        }
        if (result.size() > 0) {
            if (sort == null) {
                result.sort(count);
            } else {
                result.fieldSort(sort, exampleHitWritable.getSortFieldTypes(), count);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Time for sorting: " + (System.currentTimeMillis() - start) + " ms");
        }
        return result;
    }

    @Override
    public int count(final Query query, final String[] indexNames) throws ChokException {
        ClientResult<Integer> results = client.broadcastToIndices(ResultCompletePolicy.awaitCompletion(timeout), COUNT_METHOD, COUNT_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), null, timeout);
        if (results.isError()) {
            throw results.getChokException();
        }
        int count = 0;
        for (Integer n : results.getResults()) {
            count += n;
        }
        return count;
    }

    @Override
    public int count(final Query query, Filter filter, final String[] indexNames) throws ChokException {
        ClientResult<Integer> results = client.broadcastToIndices(ResultCompletePolicy.awaitCompletion(timeout), FILTER_COUNT_METHOD, FILTER_COUNT_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), new FilterWritable(filter), null, timeout);
        if (results.isError()) {
            throw results.getChokException();
        }
        int count = 0;
        for (Integer n : results.getResults()) {
            count += n;
        }
        return count;
    }

    protected DocumentFrequencyWritable getDocFrequencies(final Query query, final String[] indexNames) throws ChokException {
        ClientResult<DocumentFrequencyWritable> results = client.broadcastToIndices(ResultCompletePolicy.awaitCompletion(timeout), DOC_FREQ_METHOD, DOC_FREQ_METHOD_SHARD_ARG_IDX, indexNames, new QueryWritable(query), null);
        if (results.isError()) {
            throw results.getChokException();
        }
        DocumentFrequencyWritable result = null;
        for (DocumentFrequencyWritable df : results.getResults()) {
            if (result == null) {
                // Start with first result.
                result = df;
            } else {
                // Aggregate rest of results into first result.
                result.addNumDocs(df.getNumDocs());
                result.putAll(df.getAll());
            }
        }
        if (result == null) {
            result = new DocumentFrequencyWritable(); // TODO: ?
        }
        return result;
    }

    @Override
    public MapWritable getDetails(final Hit hit) throws ChokException {
        return getDetails(hit, null);
    }

    @Override
    public MapWritable getDetails(final Hit hit, final String[] fields) throws ChokException {
        List<String> shards = new ArrayList<>();
        shards.add(hit.getShard());
        int docId = hit.getDocId();
        //
        Object[] args;
        Method method;
        int shardArgIdx;
        if (fields == null) {
            args = new Object[]{null, docId};
            method = GET_DETAILS_METHOD;
            shardArgIdx = GET_DETAILS_METHOD_SHARD_ARG_IDX;
        } else {
            args = new Object[]{null, docId, fields};
            method = GET_DETAILS_FIELDS_METHOD;
            shardArgIdx = GET_DETAILS_FIELDS_METHOD_SHARD_ARG_IDX;
        }
        ClientResult<MapWritable> results = client.broadcastToShards(ResultCompletePolicy.awaitCompletion(timeout), method, shardArgIdx, shards, args);
        if (results.isError()) {
            throw results.getChokException();
        }
        return results.getResults().isEmpty() ? null : results.getResults().iterator().next();
    }

    @Override
    public List<MapWritable> getDetails(List<Hit> hits) throws ChokException, InterruptedException {
        return getDetails(hits, null);
    }

    @Override
    public List<MapWritable> getDetails(List<Hit> hits, final String[] fields) throws ChokException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(10, hits.size() + 1));
        List<MapWritable> results = new ArrayList<>();
        List<Future<MapWritable>> futures = new ArrayList<>();
        for (final Hit hit : hits) {
            futures.add(executorService.submit(() -> getDetails(hit, fields)));
        }

        for (Future<MapWritable> future : futures) {
            try {
                results.add(future.get());
            } catch (ExecutionException e) {
                throw new ChokException("Could not get hit details.", e.getCause());
            }
        }

        executorService.shutdown();

        return results;
    }

    @Override
    public double getQueryPerMinute() {
        return client.getQueryPerMinute();
    }

    @Override
    public void close() {
        client.close();
    }

    protected Class<? extends ILuceneServer> getServerClass() {
        return ILuceneServer.class;
    }
}
