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
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * The back end server which searches a set of Lucene indices. Each shard is a
 * Lucene index directory.
 * <p>
 * Normal usage is to first call getDocFreqs() to get the global term
 * frequencies, then pass that back in to search(). This way you get uniform
 * scoring across all the nodes / instances of LuceneServer.
 */
@ProtocolInfo(protocolName = "SimpleTestServer", protocolVersion = 0L)
public class SimpleTestServer implements IContentServer, ISimpleTestServer {

    private final static Logger LOG = LoggerFactory.getLogger(SimpleTestServer.class);

    protected final Set<String> shards = Collections.synchronizedSet(new HashSet<>());
    protected String nodeName;

    public static final long versionID = 0L;

    @Override
    public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
        return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return null;
    }

    @Override
    public void init(String nodeName, NodeConfiguration nodeConfiguration) {
        this.nodeName = nodeName;
    }

    public String getNodeName() {
        return nodeName;
    }

    /**
     * Adds an shard index search for given name to the list of shards
     * MultiSearcher search in.
     *
     * @param shardName the shard name
     * @param shardPath the shard directory
     * @throws java.io.IOException if and error occurs
     */
    @Override
    public void addShard(final String shardName, final Path shardPath) throws IOException {
        LOG.info("TestServer " + nodeName + " got shard " + shardName);
        Path dataFile = shardPath.resolve(TestIndex.DATA_FILE_NAME);
        if (!Files.exists(dataFile)) {
            throw new IOException("File " + dataFile + " not found");
        }
        shards.add(shardName);
    }

    @Override
    public Path replaceShard(String shardName, Path shardPath) throws Exception {
        LOG.info("TestServer " + nodeName + " got replace shard " + shardName);
        Path dataFile = shardPath.resolve(TestIndex.DATA_FILE_NAME);
        if (!Files.exists(dataFile)) {
            throw new IOException("File " + dataFile + " not found");
        }
        return null;
    }

    /**
     * Removes a search by given shardName from the list of searchers.
     * @param shardName the shard name
     */
    @Override
    public void removeShard(final String shardName) {
        LOG.info("TestServer " + nodeName + " removing shard " + shardName);
        shards.remove(shardName);
    }

    @Override
    public Collection<String> getShards() {
        return shards;
    }

    /**
     * Returns the number of documents a shard has.
     *
     * @param shardName the shard name
     * @return the number of documents in the shard.
     */
    protected int shardSize(String shardName) {
        return 0;
    }

    /**
     * Returns the disk used by the shard.
     *
     * @param shardName the shard name
     * @return the disk usage of the shard.
     */
    protected int shardDiskUsage(String shardName) {
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
     * @throws Exception if an error occurs
     */
    @Override
    public Map<String, String> getShardMetaData(String shardName) throws Exception {
        Map<String, String> metaData = new HashMap<>();
        metaData.put(SHARD_SIZE_KEY, Integer.toString(shardSize(shardName)));
        metaData.put(SHARD_DISK_USAGE_KEY, Integer.toString(shardDiskUsage(shardName)));
        return metaData;
    }

    /**
     * Close all Lucene indices. No further calls will be made after this one.
     */
    @Override
    public void shutdown() throws IOException {
        shards.clear();
    }

    @Override
    public String[] testRequest(String query, String[] shardNames) throws IOException {
        return Arrays.stream(shardNames)
                .map(shardName -> shardName.substring(shardName.indexOf("#") + 1))
                .map(shardPath -> shardPath + ":" + query).toArray(String[]::new);
    }
}
