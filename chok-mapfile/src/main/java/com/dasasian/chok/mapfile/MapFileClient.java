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
package com.dasasian.chok.mapfile;

import com.dasasian.chok.client.Client;
import com.dasasian.chok.client.ClientResult;
import com.dasasian.chok.client.INodeSelectionPolicy;
import com.dasasian.chok.util.ChokException;
import com.dasasian.chok.util.ClientConfiguration;
import com.dasasian.chok.util.ZkConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * The front end to the MapFile server.
 */
public class MapFileClient implements IMapFileClient {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(MapFileClient.class);
    private static final long TIMEOUT = 12000;
    private static final Method GET_METHOD;
    private static final int GET_METHOD_SHARD_ARG_IDX = 1;
    static {
        try {
            GET_METHOD = IMapFileServer.class.getMethod("get", new Class[]{Text.class, String[].class});
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find method get() in IMapFileServer!");
        }
    }
    private Client chokClient;

    public MapFileClient(final INodeSelectionPolicy nodeSelectionPolicy) {
        chokClient = new Client(IMapFileServer.class, nodeSelectionPolicy);
    }

    public MapFileClient() {
        chokClient = new Client(IMapFileServer.class);
    }


    //  public List<Writable> get(WritableComparable<?> key, String[] shards) throws IOException {

    public MapFileClient(final ZkConfiguration zkConfig) {
        chokClient = new Client(IMapFileServer.class, zkConfig);
    }
    public MapFileClient(final INodeSelectionPolicy policy, final ZkConfiguration czkCnfig) {
        chokClient = new Client(IMapFileServer.class, policy, czkCnfig);
    }

    public MapFileClient(final INodeSelectionPolicy policy, final ZkConfiguration zkConfig, ClientConfiguration clientConfiguration) {
        chokClient = new Client(IMapFileServer.class, policy, zkConfig, clientConfiguration);
    }

    public List<String> get(final String key, final String[] indexNames) throws ChokException {
        ClientResult<TextArrayWritable> results = chokClient.broadcastToIndices(TIMEOUT, true, GET_METHOD, GET_METHOD_SHARD_ARG_IDX, indexNames, new Text(key), null);
        if (results.isError()) {
            throw results.getChokException();
        }
        List<String> stringResults = new ArrayList<>();
        for (TextArrayWritable taw : results.getResults()) {
            for (Writable w : taw.array.get()) {
                Text text = (Text) w;
                stringResults.add(text.toString());
            }
        }
        return stringResults;
    }


    public void close() {
        chokClient.close();
    }

}
