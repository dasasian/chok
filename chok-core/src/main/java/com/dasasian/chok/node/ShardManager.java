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
package com.dasasian.chok.node;

import com.dasasian.chok.util.*;
import com.dasasian.chok.util.ThrottledInputStream.ThrottleSemaphore;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ShardManager {

    protected final static Logger LOG = LoggerFactory.getLogger(ShardManager.class);

    private final Path shardsFolder;
    private final ChokFileSystem.Factory chokFileSystemFactory;
    private final ThrottleSemaphore throttleSemaphore;

    public ShardManager(Path shardsFolder, ChokFileSystem.Factory chokFileSystemFactory, ThrottleSemaphore throttleSemaphore) {
        this.shardsFolder = shardsFolder;
        this.chokFileSystemFactory = chokFileSystemFactory;
        this.throttleSemaphore = throttleSemaphore;
        if (!Files.exists(this.shardsFolder)) {
            try {
                Files.createDirectories(this.shardsFolder);
            } catch (IOException e) {
                throw new IllegalStateException("could not create local shard folder '" + this.shardsFolder.toAbsolutePath() + "'", e);
            }
        }
    }

    public Path installShard(String shardName, URI shardUri) throws Exception {
        Path localShardFolder = getShardFolder(shardName);
        try {
            installShard(shardName, shardUri, localShardFolder);
            return localShardFolder;
        } catch (Exception e) {
            // cleanup
            FileUtil.deleteFolder(localShardFolder.toFile());
            throw e;
        }
    }

    public void uninstallShard(String shardName) {
        Path localShardFolder = getShardFolder(shardName);
        FileUtil.deleteFolder(localShardFolder.toFile());
    }

    public List<String> getInstalledShards() {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardsFolder, FileUtil.VISIBLE_PATHS_FILTER)) {
            for (Path entry: stream) {
                builder.add(entry.getFileName().toString());
            }
        } catch (IOException e) {
            LOG.error("Error while getting installed shards", e);
        }
        return builder.build();
    }

    public Path getShardsFolder() {
        return shardsFolder;
    }

    public Path getShardFolder(String shardName) {
        return shardsFolder.resolve(shardName);
    }

    /*
     * Loads a shard from the given URI. The uri is handled bye the hadoop file
     * system. So all hadoop support file systems can be used, like local hdfs s3
     * etc. In case the shard is compressed we also unzip the content. If the
     * system property chok.spool.zip.shards is true, the zip file is staged to
     * the local disk before being unzipped.
     */
    private void installShard(String shardName, URI shardUri, Path localShardFolder) throws ChokException {
        LOG.info("install shard '" + shardName + "' from " + shardUri);
        // TODO sg: to fix HADOOP-4422 we try to download the shard 5 times
        int maxTries = 5;
        for (int i = 0; i < maxTries; i++) {
            try {
                ChokFileSystem fileSystem = chokFileSystemFactory.create(shardUri);

                if (throttleSemaphore != null) {
                    fileSystem = new ThrottledFileSystem(fileSystem, throttleSemaphore);
                }

                boolean isZip = fileSystem.isFile(shardUri) && shardUri.getPath().endsWith(".zip");

                Path shardTmpFolder = Paths.get(localShardFolder.toString() + "_tmp");
                try {
                    FileUtil.deleteFolder(localShardFolder.toFile());
                    FileUtil.deleteFolder(shardTmpFolder.toFile());

                    if (isZip) {
                        FileUtil.unzip(shardUri, shardTmpFolder, fileSystem, "true".equalsIgnoreCase(System.getProperty("chok.spool.zip.shards", "false")));
                    } else {
                        fileSystem.copyToLocalFile(shardUri, shardTmpFolder);
                    }
                    Files.move(shardTmpFolder, localShardFolder);
                } finally {
                    // Ensure that the tmp folder is deleted on an error
                    FileUtil.deleteFolder(shardTmpFolder.toFile());
                }
                // Looks like we were successful.
                if (i > 0) {
                    LOG.error("Loaded shard:" + shardUri);
                }
                return;
            } catch (final Exception e) {
                LOG.error(String.format("Error loading shard: %s (try %d of %d)", shardUri, i, maxTries), e);
                if (i >= maxTries - 1) {
                    throw new ChokException("Cannot load shard: " + shardUri, e);
                }
            }
        }
    }

}
