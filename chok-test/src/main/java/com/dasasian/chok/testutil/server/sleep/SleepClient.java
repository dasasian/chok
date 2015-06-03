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
package com.dasasian.chok.testutil.server.sleep;

import com.dasasian.chok.client.Client;
import com.dasasian.chok.client.ClientResult;
import com.dasasian.chok.client.INodeSelectionPolicy;
import com.dasasian.chok.util.ChokException;
import com.dasasian.chok.util.ClientConfiguration;
import com.dasasian.chok.util.ZkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * The front end for a test server that just sleeps for a while then returns
 * nothing. Used for testing.
 */
public class SleepClient implements ISleepClient {

    protected final static Logger LOG = LoggerFactory.getLogger(SleepClient.class);
    private static final Method SLEEP_METHOD;
    private static final int SLEEP_METHOD_SHARD_ARG_IDX = 2;
    static {
        try {
            SLEEP_METHOD = ISleepServer.class.getMethod("sleep", Long.TYPE, Integer.TYPE, String[].class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method sleep() in ISleepServer!");
        }
    }
    private Client client;

    public SleepClient(final INodeSelectionPolicy nodeSelectionPolicy) {
        client = new Client(ISleepServer.class, nodeSelectionPolicy);
    }

    public SleepClient() {
        client = new Client(ISleepServer.class);
    }

    public SleepClient(final ZkConfiguration config) {
        client = new Client(ISleepServer.class, config);
    }
    public SleepClient(final INodeSelectionPolicy policy, final ZkConfiguration config) {
        client = new Client(ISleepServer.class, policy, config);
    }

    public SleepClient(final INodeSelectionPolicy policy, final ZkConfiguration config, final ClientConfiguration clientConfiguration) {
        client = new Client(ISleepServer.class, policy, config, clientConfiguration);
    }

    public int sleep(final long msec) throws ChokException {
        return sleepShards(msec, 0, null);
    }

    public int sleep(final long msec, final int delta) throws ChokException {
        return sleepShards(msec, delta, null);
    }

    public int sleepShards(final long msec, final String[] shards) throws ChokException {
        return sleepShards(msec, 0, shards);
    }

    public int sleepShards(final long msec, final int delta, final String[] shards) throws ChokException {
        ClientResult<Integer> results = client.broadcastToShards(msec + delta + 3000, true, SLEEP_METHOD, SLEEP_METHOD_SHARD_ARG_IDX, shards != null ? Arrays.asList(shards) : null, msec, delta, null);
        if (results.isError()) {
            throw results.getChokException();
        }
        int totalShards = 0;
        for (int numShards : results.getResults()) {
            totalShards += numShards;
        }
        return totalShards;
    }

    public int sleepIndices(final long msec, final String[] indices) throws ChokException {
        return sleepIndices(msec, 0, indices);
    }

    public int sleepIndices(final long msec, final int delta, final String[] indices) throws ChokException {
        ClientResult<Integer> results = client.broadcastToIndices(msec + delta + 3000, true, SLEEP_METHOD, SLEEP_METHOD_SHARD_ARG_IDX, indices, msec, delta, null);
        if (results.isError()) {
            throw results.getChokException();
        }
        int totalShards = 0;
        for (int numShards : results.getResults()) {
            totalShards += numShards;
        }
        return totalShards;
    }

    public Client getClient() {
        return client;
    }

    public void close() {
        client.close();
    }

}
