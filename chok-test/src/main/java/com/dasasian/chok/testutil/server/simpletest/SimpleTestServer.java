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

import com.dasasian.chok.node.IContentServer;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.util.NodeConfiguration;
import com.google.common.collect.ImmutableSet;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * The back end server which searches a set of Lucene indices. Each shard is a
 * Lucene index directory.
 * <p>
 * Normal usage is to first call getDocFreqs() to get the global term
 * frequencies, then pass that back in to search(). This way you get uniform
 * scoring across all the nodes / instances of LuceneServer.
 */
public class SimpleTestServer implements IContentServer, ISimpleTestServer {

    private final static Logger LOG = Logger.getLogger(SimpleTestServer.class);

    protected String _nodeName;

    public SimpleTestServer() {
        // default way of initializing an IContentServer
    }

    @Override
    public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
        return 0L;
    }

    @Override
    public void init(String nodeName, NodeConfiguration nodeConfiguration) {
        _nodeName = nodeName;
    }

    public String getNodeName() {
        return _nodeName;
    }

    /**
     * Adds an shard index search for given name to the list of shards
     * MultiSearcher search in.
     *
     * @param shardName
     * @param shardDir
     * @throws java.io.IOException
     */
    @Override
    public void addShard(final String shardName, final File shardDir) throws IOException {
        LOG.info("TestServer " + _nodeName + " got shard " + shardName);
        File dataFile = new File(shardDir, TestIndex.DATA_FILE_NAME);
        if (!dataFile.exists()) {
            throw new IOException("File " + dataFile + " not found");
        }
    }

    /**
     * Removes a search by given shardName from the list of searchers.
     */
    @Override
    public void removeShard(final String shardName) {
        LOG.info("TestServer " + _nodeName + " removing shard " + shardName);
    }

    @Override
    public Collection<String> getShards() {
        return ImmutableSet.of();
    }

    /**
     * Returns the number of documents a shard has.
     *
     * @param shardName
     * @return the number of documents in the shard.
     */
    protected int shardSize(String shardName) {
        return 0;
    }

    /**
     * Returns data about a shard. Currently the only standard key is
     * SHARD_SIZE_KEY. This value will be reported by the listIndexes command. The
     * units depend on the type of server. It is OK to return an empty map or
     * null.
     *
     * @param shardName The name of the shard to measure. This was the name provided in
     *                  addShard().
     * @return a map of key/value pairs which describe the shard.
     * @throws Exception
     */
    @Override
    public Map<String, String> getShardMetaData(String shardName) throws Exception {
        Map<String, String> metaData = new HashMap<>();
        metaData.put(SHARD_SIZE_KEY, Integer.toString(shardSize(shardName)));
        return metaData;
    }

    /**
     * Close all Lucene indices. No further calls will be made after this one.
     */
    @Override
    public void shutdown() throws IOException {
    }

    @Override
    public String testRequest(String query, String[] shardNames) throws IOException {
        return query;
    }
}
