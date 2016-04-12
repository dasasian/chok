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
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.FileUtil;
import com.dasasian.chok.util.ThrottledFileSystem;
import com.dasasian.chok.util.ThrottledInputStream.ThrottleSemaphore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Predicate;

public class ShardManager {

    protected final static Logger LOG = LoggerFactory.getLogger(ShardManager.class);

    public static final char LAST_MODIFIED_SHARD_NAME_SEPARATOR = '@';
    public static final String TEMP_SHARD_EXTENSION = "_tmp";

    private final Path shardsFolder;
    private final ChokFileSystem.Factory chokFileSystemFactory;
    private final ThrottleSemaphore throttleSemaphore;

    private final Map<String, URI> autoReloadShards = Maps.newHashMap();

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

    public Path installShard(String shardName, URI shardUri, boolean autoReload) throws Exception {
        Path localShardFolder = getShardFolder(shardName, getLastModified(shardUri));
        try {
            installShard(shardName, shardUri, localShardFolder);
            if (autoReload) {
                installReloadCheck(shardName, shardUri);
            }
            return localShardFolder;
        } catch (Exception e) {
            // cleanup
            FileUtil.deleteFolder(localShardFolder.toFile());
            throw e;
        }
    }

    public long getLastModified(URI shardUri) throws IOException {
        ChokFileSystem chokFileSystem = chokFileSystemFactory.create(shardUri);
        return chokFileSystem.lastModified(shardUri);
    }

    public void removeLocalShardFolder(Path localShardFolder) {
        FileUtil.deleteFolder(localShardFolder.toFile());
    }

    public void uninstallShard(String shardName) {
        for (Path localShardFolder : getLocalShardFolders(shardName)) {
            synchronized (shardName.intern()) {
                FileUtil.deleteFolder(localShardFolder.toFile());
            }
        }
    }

    public Iterable<Path> getLocalShardFolders(@Nullable String shardName) {
        final DirectoryStream.Filter<Path> pathsFilter = entry -> {
            if (FileUtil.VISIBLE_PATHS_FILTER.accept(entry)) {
                if (shardName != null) {
                    return entry.getFileName().toString().equals(shardName) ||
                            entry.getFileName().toString().startsWith(shardName + LAST_MODIFIED_SHARD_NAME_SEPARATOR);
                }
                return true;
            }
            return false;
        };
        try {
            return Files.newDirectoryStream(shardsFolder, pathsFilter);
        } catch (IOException e) {
            LOG.error("Error while getting local shards", e);
        }
        return ImmutableList.of();
    }

    public Path getShardsFolder() {
        return shardsFolder;
    }

    public Path getShardFolder(String shardName, long lastModified) {
        return getShardsFolder().resolve(shardName + LAST_MODIFIED_SHARD_NAME_SEPARATOR + lastModified);
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
                synchronized (shardName.intern()) {
                    ChokFileSystem fileSystem = chokFileSystemFactory.create(shardUri);

                    if (throttleSemaphore != null) {
                        fileSystem = new ThrottledFileSystem(fileSystem, throttleSemaphore);
                    }

                    boolean isZip = fileSystem.isFile(shardUri) && shardUri.getPath().endsWith(".zip");

                    Path shardTmpFolder = Paths.get(localShardFolder.toString() + TEMP_SHARD_EXTENSION);
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
                }
            } catch (final Exception e) {
                LOG.error(String.format("Error loading shard: %s (try %d of %d)", shardUri, i, maxTries), e);
                if (i >= maxTries - 1) {
                    throw new ChokException("Cannot load shard: " + shardUri, e);
                }
            }
        }
    }

    public Map<String, URI> getLocalShards() {
        Map<String, Path> localShards = Maps.newHashMap();
        for (Path localShardFolder : getLocalShardFolders(null)) {
            String shardName = getShardName(localShardFolder);
            long currentLastModified = (localShards.containsKey(shardName)) ? getLastModified(localShards.get(shardName)) : 0;
            long lastModified = getLastModified(localShardFolder);
            if (lastModified > currentLastModified) {
                localShards.put(shardName, localShardFolder);
            }
        }
        return Maps.transformValues(localShards, Path::toUri);
    }

    private long getLastModified(Path localShardFolder) {
        String folderName = localShardFolder.getFileName().toString();
        return getLastModified(folderName);
    }

    protected long getLastModified(String folderName) {
        final int startIndex = folderName.indexOf(LAST_MODIFIED_SHARD_NAME_SEPARATOR);
        if (startIndex > 0) {
            try {
                return Long.parseLong(folderName.substring(startIndex+1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(folderName + " is not a valid local shard folder name");
            }
        }
        return 0L;
    }

    private String getShardName(Path localShardFolder) {
        String folderName = localShardFolder.getFileName().toString();
        final int endIndex = folderName.indexOf(LAST_MODIFIED_SHARD_NAME_SEPARATOR);
        if (endIndex > 0) {
            return folderName.substring(0, endIndex);
        }
        return folderName;
    }

    public long getLocalShardLastModified(String shardName) {
        Path localShardFolder = Iterables.getOnlyElement(getLocalShardFolders(shardName));
        return getLastModified(localShardFolder.getFileName().toString());
    }

    private void installReloadCheck(final String shardName, final URI shardUri) {
        autoReloadShards.put(shardName, shardUri);
    }

    public Map<String, URI> getReloadShards(NodeContext context) {
        if(!autoReloadShards.isEmpty()) {
            Predicate<Map.Entry<String, URI>> reloadPredicate = shardNameUriEntry -> {
                final String shardName = shardNameUriEntry.getKey();
                final URI shardUri = shardNameUriEntry.getValue();
                try {
                    final long lastModified = context.getShardManager().getLastModified(shardUri);
                    final long localLastModified = context.getShardManager().getLocalShardLastModified(shardName);
                    return lastModified > localLastModified;
                } catch (IOException e) {
                    LOG.warn("Unable to get last modified from " + shardUri);
                }
                return false;
            };
            return Maps.filterEntries(autoReloadShards, reloadPredicate::test);
        }
        return ImmutableMap.of();
    }

    public void cleanup() {
        final DirectoryStream.Filter<Path> pathsFilter = entry -> FileUtil.VISIBLE_PATHS_FILTER.accept(entry) &&
                entry.getFileName().toString().endsWith(TEMP_SHARD_EXTENSION);
        try {
            for(Path path : Files.newDirectoryStream(shardsFolder, pathsFilter)) {
                FileUtil.deleteFolder(path.toFile());
            }
        } catch (IOException e) {
            LOG.error("Error while cleaning up", e);
        }
    }
}
