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

import com.dasasian.chok.node.IContentServer;
import com.dasasian.chok.util.NodeConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/**
 * Implements search over a set of Hadoop <code>MapFile</code>s.
 */
@ProtocolInfo(protocolName = "MapFileServer", protocolVersion = 0L)
public class MapFileServer implements IContentServer, IMapFileServer {

    private final static Logger LOG = LoggerFactory.getLogger(MapFileServer.class);

    private final Configuration conf = new Configuration();
    private final Map<String, MapFile.Reader> readerByShard = new ConcurrentHashMap<>();
    private final Map<String, Path> pathByShard = new ConcurrentHashMap<>();
    private String nodeName;

    @Override
    public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
        return 0L;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return null;
    }

    @Override
    public void init(String nodeName, NodeConfiguration nodeConfiguration) {
        this.nodeName = nodeName;
    }

    /**
     * Adds an shard index search for given name to the list of shards
     * MultiSearcher search in.
     *
     * @param shardName the shard name
     * @param shardDir the shard dir
     * @throws IOException when an error occurs
     */
    @Override
    public void addShard(final String shardName, final Path shardDir) throws IOException {
        LOG.debug("LuceneServer " + nodeName + " got shard " + shardName);
        if (!Files.exists(shardDir)) {
            throw new IOException("Shard " + shardName + " dir " + shardDir + " does not exist!");
        }
        if (!Files.isReadable(shardDir)) {
            throw new IOException("Can not read shard " + shardName + " dir " + shardDir + "!");
        }
        try {
            final Reader reader = openReader(shardDir);
            pathByShard.put(shardName, shardDir);
            readerByShard.put(shardName, reader);
        } catch (IOException e) {
            LOG.error("Error opening shard " + shardName + " " + shardDir, e);
            throw e;
        }
    }

    private Reader openReader(Path shardDir) throws IOException {
        return new Reader(new org.apache.hadoop.fs.Path(shardDir.toUri()), conf);
    }

    @Override
    public Path replaceShard(String shardName, Path shardDir) throws Exception {
        LOG.debug("LuceneServer " + nodeName + " got replace shard " + shardName);
        if (!Files.exists(shardDir)) {
            throw new IOException("Shard " + shardName + " dir " + shardDir + " does not exist!");
        }
        if (!Files.isReadable(shardDir)) {
            throw new IOException("Can not read shard " + shardName + " dir " + shardDir + "!");
        }
        try {
            final MapFile.Reader oldReader = readerByShard.get(shardName);
            final Path oldPath = pathByShard.get(shardName);

            final Reader reader = openReader(shardDir);
            pathByShard.put(shardName, shardDir);
            readerByShard.put(shardName, reader);

            if (oldReader != null) {
                closeReader(shardName, oldReader);
            }

            return oldPath;
        } catch (IOException e) {
            LOG.error("Error opening shard " + shardName + " " + shardDir, e);
            throw e;
        }

    }

    @Override
    public Collection<String> getShards() {
        return Collections.unmodifiableCollection(readerByShard.keySet());
    }

    /**
     * Removes a search by given shardName from the list of searchers.
     */
    public void removeShard(final String shardName) throws IOException {
        LOG.debug("LuceneServer " + nodeName + " removing shard " + shardName);
        synchronized (readerByShard) {
            pathByShard.remove(shardName);
            final MapFile.Reader reader = readerByShard.get(shardName);
            if (reader != null) {
                closeReader(shardName, reader);
                readerByShard.remove(shardName);
            } else {
                LOG.warn("Shard " + shardName + " not found!");
            }

        }
    }

    private void closeReader(String shardName, Reader reader) throws IOException {
        try {
            reader.close();
        } catch (IOException e) {
            LOG.error("Error closing shard " + shardName, e);
            throw e;
        }
    }

    /**
     * Returns the amount of disk used by the shard.
     *
     * @param shardName name of the shard
     * @return the amount of disk used by the shard
     */
    protected long shardDiskUsage(String shardName) {
        Path shardDir = pathByShard.get(shardName);
        if (shardDir != null) {
            try {
                return FileUtils.sizeOfDirectory(shardDir.toFile());
            } catch (Exception ignore) {
            }
        }
        throw new IllegalArgumentException("Shard '" + shardName + "' unknown");
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
     * @throws Exception when and error occurs
     */
    public Map<String, String> getShardMetaData(String shardName) throws Exception {
        Map<String, String> metaData = new HashMap<>();
        metaData.put(SHARD_SIZE_KEY, Integer.toString(shardSize(shardName)));
        metaData.put(SHARD_DISK_USAGE_KEY, Long.toString(shardDiskUsage(shardName)));
        return metaData;
    }

    private int shardSize(String shardName) throws IOException, InstantiationException, IllegalAccessException {
        final Reader reader = readerByShard.get(shardName);
        if (reader != null) {
            int count = 0;
            synchronized (reader) {
                reader.reset();
                WritableComparable<?> key = (WritableComparable<?>) reader.getKeyClass().newInstance();
                Writable value = (Writable) reader.getValueClass().newInstance();
                while (reader.next(key, value)) {
                    count++;
                }
            }
            return count;
        }
        throw new IllegalArgumentException("Shard '" + shardName + "' unknown");
    }

    /**
     * Close all MapFiles. No further calls will be made after this one.
     */
    public void shutdown() throws IOException {
        for (final MapFile.Reader reader : readerByShard.values()) {
            try {
                reader.close();
            } catch (IOException e) {
                LOG.error("Error in shutdown", e);
            }
        }
        readerByShard.clear();
    }

    public TextArrayWritable get(Text key, String[] shards) throws IOException {
        ExecutorService executor = Executors.newCachedThreadPool();
        Collection<Future<Text>> futures = new ArrayList<>();
        for (String shard : shards) {
            final MapFile.Reader reader = readerByShard.get(shard);
            if (reader == null) {
                LOG.warn("Shard " + shard + " unknown");
                continue;
            }
            Callable<Text> callable = new MapLookup(reader, key);
            futures.add(executor.submit(callable));
        }
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES); // TODO: config, 10 sec?
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting on MapLookup threads", e);
        }
        executor.shutdownNow();
        List<Text> resultList = new ArrayList<>();
        for (Future<Text> future : futures) {
            try {
                Text result = future.get(0, TimeUnit.MILLISECONDS);
                if (result != null) {
                    resultList.add(result);
                }
            } catch (ExecutionException e) {
        /*
         * This MapFile red threw an exception. Stop processing and throw an
         * IOE.
         */
                Throwable t = e.getCause();
                if (t instanceof IOException) {
                    // Throw the same IOException that the MapFile.Reader threw.
                    throw (IOException) t;
                }
                // Wrap MapFile.Reader's exception in an IOException.
                throw new IOException("Error in MapLookup", t);
            } catch (TimeoutException e) {
        /*
         * Result is not ready. Should not happen, because future is done.
         * Continue as if MapLookup had returned null.
         */
                LOG.warn("Timed out while getting MapLookup", e);
            } catch (InterruptedException e) {
        /*
         * Something went wrong while waiting for result. Should not happen
         * because we wait for 0 msec, and the future is done. Continue as if
         * the MapLookup had returned null.
         */
                LOG.warn("Interrupted while getting RPC result", e);
            }
        }
        return new TextArrayWritable(resultList);
    }

    private class MapLookup implements Callable<Text> {

        private final MapFile.Reader reader;
        private final WritableComparable<?> key;

        public MapLookup(Reader reader, WritableComparable<?> key) {
            this.reader = reader;
            this.key = key;
        }

        public Text call() throws Exception {
            synchronized (reader) {
                Writable result = (Writable) reader.getValueClass().newInstance();
                result = reader.get(key, result);
                return (Text) result;
            }
        }

    }

}
