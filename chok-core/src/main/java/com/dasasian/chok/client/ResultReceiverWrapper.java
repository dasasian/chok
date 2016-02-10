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
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

/**
 * These are the only ClientResult methods NodeInteraction is allowed to call.
 */
public class ResultReceiverWrapper<T> implements IResultReceiverWrapper<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ResultReceiverWrapper.class);

    private final ImmutableSet<String> allShards;
    private final Set<String> seenShards = Sets.newConcurrentHashSet();
    private final IResultReceiver<T> resultReceiver;

    private boolean isClosed = false;

    public ResultReceiverWrapper(@Nonnull ImmutableSet<String> allShards, IResultReceiver<T> resultReceiver) {
        if (allShards.isEmpty()) {
            throw new IllegalArgumentException("No shards specified");
        }
        this.allShards = allShards;
        this.resultReceiver = resultReceiver;
    }

    /**
     * @return true if the result is closed, and therefore not accepting any new
     * results.
     */
    @Override
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * @return true if we have seen either a result or an error for all shards.
     */
    @Override
    public boolean isComplete() {
        return seenShards.containsAll(allShards);
    }

//    /**
//     * @return true if any errors were reported.
//     */
//    public synchronized boolean isError() {
//        return !errors.isEmpty();
//    }
//
//    /**
//     * @return true if result is complete (all shards reporting in) and no errors
//     * occurred.
//     */
//    public synchronized boolean isOK() {
//        return isComplete() && !isError();
//    }
//
//    /**
//     * @return the set of shards from whom we have seen either results or errors.
//     */
//    public synchronized Set<String> getSeenShards() {
//        return Collections.unmodifiableSet(isClosed ? seenShards : new HashSet<>(seenShards));
//    }
//
//    /**
//     * @return the subset of all shards from whom we have not seen either results
//     * or errors.
//     */
//    public synchronized Set<String> getMissingShards() {
//        Set<String> missing = new HashSet<>(allShards);
//        missing.removeAll(seenShards);
//        return missing;
//    }

    /**
     * @return the ratio (0.0 .. 1.0) of shards we have seen. 0.0 when no shards,
     * 1.0 when complete.
     */
    @Override
    public synchronized double getShardCoverage() {
        int seen = seenShards.size();
        int all = allShards.size();
        return all > 0 ? (double) seen / (double) all : 0.0;
    }

    @Override
    public synchronized void close() {
        isClosed = true;
        if (LOG.isTraceEnabled()) {
            LOG.trace("close() called.");
        }
        notifyAll();
    }

    private boolean doAdd(final Collection<String> shards) {
        if (isClosed) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Ignoring results given to closed ClientResult");
            }
            return false;
        }
        if (shards == null) {
            LOG.warn("Null shards passed to AddResult()");
            return false;
        }
        if (shards.isEmpty()) {
            LOG.warn("Empty shards passed to AddResult()");
            return false;
        }
        return true;
    }

    @Override
    public void addNodeResult(T result, Set<String> shards) {
        if(doAdd(shards)) {
            resultReceiver.addResult(result, shards);

            if (LOG.isWarnEnabled()) {
                for (String shard : shards) {
                    if (seenShards.contains(shard)) {
                        LOG.warn("Duplicate occurrences of shard " + shard);
                    } else if (!allShards.contains(shard)) {
                        LOG.warn("Unknown shard " + shard + " returned results");
                    }
                }
            }

            seenShards.addAll(shards);

            synchronized (this) {
                notifyAll();
            }
        }
    }

    @Override
    public void addNodeError(Throwable error, Set<String> shards) {
        if(doAdd(shards)) {
            resultReceiver.addError(error, shards);

            if (LOG.isWarnEnabled()) {
                for (String shard : shards) {
                    if (seenShards.contains(shard)) {
                        LOG.warn("Duplicate occurrences of shard " + shard);
                    } else if (!allShards.contains(shard)) {
                        LOG.warn("Unknown shard " + shard + " returned results");
                    }
                }
            }

            seenShards.addAll(shards);

            synchronized (this) {
                notifyAll();
            }
        }
    }

    @Override
    public Set<String> getAllShards() {
        return allShards;
    }

    @Override
    public Set<String> getMissingShards() {
        return allShards.stream().filter(shard -> !seenShards.contains(shard)).collect(Collectors.toSet());
    }

    @Override
    public Set<String> getSeenShards() {
        return ImmutableSet.copyOf(seenShards);
    }
}
