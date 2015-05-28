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
package com.dasasian.chok.testutil.server.simpletest;

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
import java.util.List;

/**
 * The front end for a test server that just sleeps for a while then returns
 * nothing. Used for testing.
 */
public class SimpleTestClient implements ISimpleTestClient {

    protected final static Logger LOG = LoggerFactory.getLogger(SimpleTestClient.class);
    private static final Method SLEEP_METHOD;
    private static final int SLEEP_METHOD_SHARD_ARG_IDX = 1;
    static {
        try {
            SLEEP_METHOD = ISimpleTestServer.class.getMethod("testRequest", new Class[]{String.class, String[].class});
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method testRequest() in ITestServer!");
        }
    }
    private Client client;

    public SimpleTestClient(final INodeSelectionPolicy nodeSelectionPolicy) {
        client = new Client(ISimpleTestServer.class, nodeSelectionPolicy);
    }

    public SimpleTestClient() {
        client = new Client(ISimpleTestServer.class);
    }

    public SimpleTestClient(final ZkConfiguration config) {
        client = new Client(ISimpleTestServer.class, config);
    }
    public SimpleTestClient(final INodeSelectionPolicy policy, final ZkConfiguration config) {
        client = new Client(ISimpleTestServer.class, policy, config);
    }

    public SimpleTestClient(final INodeSelectionPolicy policy, final ZkConfiguration config, final ClientConfiguration clientConfiguration) {
        client = new Client(ISimpleTestServer.class, policy, config, clientConfiguration);
    }

    @Override
    public String testRequest(final String query) throws ChokException {
        return testRequest(query, null);
    }

    public String testRequest(final String query, final String[] shards) throws ChokException {
        List<String> shardList = shards != null ? Arrays.asList(shards) : null;
        ClientResult<String> results = client.broadcastToShards(3000, true, SLEEP_METHOD, SLEEP_METHOD_SHARD_ARG_IDX, shardList, query, null);
        if (results.isError()) {
            throw results.getChokException();
        }
        for (String result : results.getResults()) {
            return result;
        }
        return null;
    }

    public Client getClient() {
        return client;
    }

    public void close() {
        client.close();
    }

}
