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

import com.dasasian.chok.util.ChokException;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A multithreaded destination for results and/or errors. Results are produced
 * by nodes and we pass lists of shards to nodes. But due to replication and
 * retries, we associate sets of shards with the results, not nodes.
 * <br>
 * Multiple NodeInteractions will be writing to this object at the same time. If
 * not closed, expect the contents to change. For example isComplete() might
 * return false and then a call to getResults() might return a complete set (in
 * which case another call to isComplete() would return true). If you need
 * complex state information, rather than making multiple calls, you should use
 * <br>
 * You can get these results from a WorkQueue by polling or blocking. Once you
 * have an ClientResult instance you may poll it or block on it. Whenever
 * resutls or errors are added notifyAll() is called. The ClientResult can
 * report on the number or ratio of shards completed. You can stop the search by
 * calling close(). The ClientResult will no longer change, and any outstanding
 * threads will be killed (via notification to the provided IClosedListener).
 */
public class ClientResult<T> implements IResultReceiver<T>, Iterable<ClientResult<T>.Entry> {

    private static final Logger LOG = LoggerFactory.getLogger(ClientResult.class);
    private final Set<Entry> entries;
    private final Map<Object, Entry> resultMap;
    private final List<T> results;
    private final List<Throwable> errors;
    private final long startTime;
    private final ResultReceiverWrapper<T> resultReceiverWrapper;

    /**
     * Construct a non-closed ClientResult, which waits for addResults() or
     * addNodeError() calls until close() is called. After that point, addResults()
     * and addNodeError() calls are ignored, and this object becomes immutable.
     *
     * @param allShards      The set of all shards to expect results from.
     */
    public ClientResult(ImmutableSet<String> allShards) {
        this.resultReceiverWrapper = new ResultReceiverWrapper<>(allShards, this);
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Created ClientResult(%s)", allShards));
        }
        startTime = System.currentTimeMillis();
        this.entries = Sets.newConcurrentHashSet();
        this.resultMap = Maps.newConcurrentMap();
        this.results = Collections.synchronizedList(Lists.newArrayList());
        this.errors = Collections.synchronizedList(Lists.newArrayList());
    }

    /**
     * Add a result. Will be ignored if closed.
     *
     * @param result The result to add.
     * @param shards The shards used to compute the result.
     */
    @Override
    public void addResult(T result, Set<String> shards) {
        Entry entry = new Entry(result, shards, false);
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Adding result %s", entry));
        }
        entries.add(entry);

        if (result != null) {
            results.add(result);
            resultMap.put(result, entry);
        }
    }

    /**
     * Add an error. Will be ignored if closed.
     *
     * @param error  The error to add.
     * @param shards The shards used when the error happened.
     */
    @Override
    public void addError(Throwable error, Set<String> shards) {
        Entry entry = new Entry(error, shards, true);
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Adding error %s", entry));
        }
        entries.add(entry);
        if (error != null) {
            errors.add(error);
            resultMap.put(error, entry);
        }
    }

    /**
     * @return the set of all shards we are expecting results from.
     */
    public Set<String> getAllShards() {
        return resultReceiverWrapper.getAllShards();
    }

    public boolean isClosed() {
        return resultReceiverWrapper.isClosed();
    }

    /**
     * @return true if we have seen either a result or an error for all shards.
     */
    public boolean isComplete() {
        return resultReceiverWrapper.isComplete();
    }

    public double getShardCoverage() {
        return resultReceiverWrapper.getShardCoverage();
    }

    public void addNodeResult(T result, Set<String> shards) {
        resultReceiverWrapper.addNodeResult(result, shards);
    }

    public void addNodeError(Throwable error, Set<String> shards) {
        resultReceiverWrapper.addNodeError(error, shards);
    }

    /**
     * @return true if any errors were reported.
     */
    public synchronized boolean isError() {
        return !errors.isEmpty();
    }

    /**
     * @return all of the results seen so far. Does not include errors.
     */
    public synchronized Collection<T> getResults() {
        return Collections.unmodifiableCollection(isClosed() ? results : new ArrayList<>(results));
    }

//    /**
//     * Either return results or throw an exception. Allows simple one line use of
//     * a ClientResult. If no errors occurred, returns same results as
//     * getResults(). If any errors occurred, one is chosen via getError() and
//     * thrown.
//     *
//     * @return if no errors occurred, results via getResults().
//     * @throws Throwable if any errors occurred, via getError().
//     */
//    public synchronized Collection<T> getResultsOrThrowException() throws Throwable {
//        if (isError()) {
//            throw getError();
//        } else {
//            return getResults();
//        }
//    }
//
//    /**
//     * Either return results or throw a ChokException. Allows simple one line use
//     * of a ClientResult. If no errors occurred, returns same results as
//     * getResults(). If any errors occurred, one is chosen via getChokException()
//     * and thrown.
//     *
//     * @return if no errors occurred, results via getResults().
//     * @throws com.dasasian.chok.util.ChokException if any errors occurred, via getError().
//     */
//    public synchronized Collection<T> getResultsOrThrowChokException() throws ChokException {
//        if (isError()) {
//            throw getChokException();
//        } else {
//            return getResults();
//        }
//    }

    /**
     * @return all of the errors seen so far.
     */
    public synchronized Collection<Throwable> getErrors() {
        return Collections.unmodifiableCollection(isClosed() ? errors : new ArrayList<>(errors));
    }

    /**
     * @return a randomly chosen error, or null if none exist.
     */
    public synchronized Throwable getError() {
        for (Entry e : entries) {
            if (e.error != null) {
                return e.error;
            }
        }
        return null;
    }

    /**
     * @return a randomly chosen ChokException if one exists, else a
     * ChokException wrapped around a randomly chosen error if one
     * exists, else null.
     */
    public synchronized ChokException getChokException() {
        Throwable error = null;
        for (Entry e : this) {
            if (e.error != null) {
                if (e.error instanceof ChokException) {
                    return (ChokException) e.error;
                } else {
                    error = e.error;
                }
            }
        }
        if (error != null) {
            return new ChokException("Error", error);
        } else {
            return null;
        }
    }

//    /**
//     * @param result The result to look up.
//     * @return What shards produced the result, and when it arrived. Returns null
//     * if result not found.
//     */
//    public synchronized Entry getResultEntry(T result) {
//        return resultMap.get(result);
//    }
//
//    /**
//     * @param error The error to look up.
//     * @return What shards produced the error, and when it arrived. Returns null
//     * if error not found.
//     */
//    public synchronized Entry getErrorEntry(Throwable error) {
//        return resultMap.get(error);
//    }

    /**
     * @return the time when this ClientResult was created.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @return a snapshot of all the data about the results so far.
     */
    public Set<Entry> entrySet() {
        if (isClosed()) {
            return Collections.unmodifiableSet(entries);
        } else {
            synchronized (this) {
                // Set will keep changing, make a snapshot.
                return Collections.unmodifiableSet(new HashSet<>(entries));
            }
        }
    }

    /**
     * @return an iterator of our Entries sees so far.
     */
    @Override
    public Iterator<Entry> iterator() {
        return entrySet().iterator();
    }

    /**
     * @return a list of our results or errors, in the order they arrived.
     */
    public List<Entry> getArrivalTimes() {
        List<Entry> arrivals;
        synchronized (this) {
            arrivals = new ArrayList<>(entries);
        }
        Collections.sort(arrivals, (o1, o2) -> {
            if (o1.time != o2.time) {
                return o1.time < o2.time ? -1 : 1;
            } else {
                // Break ties in favor of results.
                if (o1.result != null && o2.result == null) {
                    return -1;
                } else if (o2.result != null && o1.result == null) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
        return arrivals;
    }

    @Override
    public synchronized String toString() {
        int numResults = 0;
        int numErrors = 0;
        for (Entry e : this) {
            if (e.result != null) {
                numResults++;
            }
            if (e.error != null) {
                numErrors++;
            }
        }
        return String.format("ClientResult: %d results, %d errors, %d/%d shards%s%s", numResults, numErrors, getSeenShards().size(), getAllShards().size(), isClosed() ? " (closed)" : "", isComplete() ? " (complete)" : "");
    }

    public Set<String> getSeenShards() {
        return resultReceiverWrapper.getSeenShards();
    }

    public Set<String> getMissingShards() {
        return resultReceiverWrapper.getMissingShards();
    }

    @Override
    public void close() {
        resultReceiverWrapper.close();
    }

    public ResultReceiverWrapper<T> getResultReceiverWrapper() {
        return resultReceiverWrapper;
    }

    /**
     * Immutable storage of either a result or an error, which shards produced it,
     * and it's arrival time.
     */
    public class Entry {

        public final T result;
        public final Throwable error;
        public final Set<String> shards;
        public final long time;

        @SuppressWarnings("unchecked")
        private Entry(Object o, Collection<String> shards, boolean isError) {
            this.result = !isError ? (T) o : null;
            this.error = isError ? (Throwable) o : null;
            this.shards = Collections.unmodifiableSet(new HashSet<>(shards));
            this.time = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            String resultStr;
            if (result != null) {
                try {
                    resultStr = result.toString();
                } catch (Throwable t) {
                    LOG.trace("Error calling toString() on result", t);
                    resultStr = "(toString() err)";
                }
                if (resultStr == null) {
                    resultStr = "(null toString())";
                }
            } else {
                resultStr = error != null ? error.getClass().getSimpleName() : "null";
            }
            return String.format("%s from %s at %d", resultStr, shards, time);
        }
    }

}
