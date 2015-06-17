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

import com.dasasian.chok.util.ChokException;
import com.dasasian.chok.util.FileUtil;
import com.dasasian.chok.util.ThrottledInputStream.ThrottleSemaphore;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import com.dasasian.chok.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

public class ShardManager {

    protected final static Logger LOG = LoggerFactory.getLogger(ShardManager.class);
    private final File shardsFolder;
    private final ThrottleSemaphore throttleSemaphore;

    public ShardManager(File shardsFolder) {
        this(shardsFolder, null);
    }

    public ShardManager(File shardsFolder, ThrottleSemaphore throttleSemaphore) {
        this.shardsFolder = shardsFolder;
        this.throttleSemaphore = throttleSemaphore;
        if (!this.shardsFolder.exists()) {
            this.shardsFolder.mkdirs();
        }
        if (!this.shardsFolder.exists()) {
            throw new IllegalStateException("could not create local shard folder '" + this.shardsFolder.getAbsolutePath() + "'");
        }
    }

    public File installShard(String shardName, String shardPath) throws Exception {
        File localShardFolder = getShardFolder(shardName);
        try {
            if (!localShardFolder.exists()) {
                installShard(shardName, shardPath, localShardFolder);
            }
            return localShardFolder;
        } catch (Exception e) {
            FileUtil.deleteFolder(localShardFolder);
            throw e;
        }
    }

    public void uninstallShard(String shardName) {
        File localShardFolder = getShardFolder(shardName);
        FileUtil.deleteFolder(localShardFolder);
    }

    public Collection<String> getInstalledShards() {
        String[] folderList = shardsFolder.list(FileUtil.VISIBLE_FILES_FILTER);
        if (folderList == null) {
            return ImmutableList.of();
        }
        return Arrays.asList(folderList);
    }

    public File getShardsFolder() {
        return shardsFolder;
    }

    public File getShardFolder(String shardName) {
        return new File(shardsFolder, shardName);
    }

    /*
     * Loads a shard from the given URI. The uri is handled bye the hadoop file
     * system. So all hadoop support file systems can be used, like local hdfs s3
     * etc. In case the shard is compressed we also unzip the content. If the
     * system property chok.spool.zip.shards is true, the zip file is staged to
     * the local disk before being unzipped.
     */
    private void installShard(String shardName, String shardPath, File localShardFolder) throws ChokException {
        LOG.info("install shard '" + shardName + "' from " + shardPath);
        // TODO sg: to fix HADOOP-4422 we try to download the shard 5 times
        int maxTries = 5;
        for (int i = 0; i < maxTries; i++) {
            URI uri;
            try {
                uri = ChokFileSystem.getURI(shardPath);
                ChokFileSystem fileSystem = (throttleSemaphore != null) ?
                        ChokFileSystem.getThrottled(uri, new Configuration(), throttleSemaphore) :
                        ChokFileSystem.get(uri, new Configuration());

//                final Path path = new Path(shardPath);
                boolean isZip = fileSystem.isFile(uri) && shardPath.endsWith(".zip");

                File shardTmpFolder = new File(localShardFolder.getAbsolutePath() + "_tmp");
                try {
                    FileUtil.deleteFolder(localShardFolder);
                    FileUtil.deleteFolder(shardTmpFolder);

                    if (isZip) {
                        FileUtil.unzip(uri, shardTmpFolder, fileSystem, "true".equalsIgnoreCase(System.getProperty("chok.spool.zip.shards", "false")));
                    } else {
                        fileSystem.copyToLocalFile(uri, shardTmpFolder.toURI());
                    }
                    shardTmpFolder.renameTo(localShardFolder);
                } finally {
                    // Ensure that the tmp folder is deleted on an error
                    FileUtil.deleteFolder(shardTmpFolder);
                }
                // Looks like we were successful.
                if (i > 0) {
                    LOG.error("Loaded shard:" + shardPath);
                }
                return;
            } catch (final URISyntaxException e) {
                throw new ChokException("Can not parse uri for path: " + shardPath, e);
            } catch (final Exception e) {
                LOG.error(String.format("Error loading shard: %s (try %d of %d)", shardPath, i, maxTries), e);
                if (i >= maxTries - 1) {
                    throw new ChokException("Can not load shard: " + shardPath, e);
                }
            }
        }
    }

}
