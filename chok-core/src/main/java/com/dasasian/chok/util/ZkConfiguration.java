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
package com.dasasian.chok.util;

import java.io.Serializable;

public class ZkConfiguration implements Serializable {

    public static final Character PATH_SEPARATOR = '/';

    private static final String NODES = "nodes";
    private static final String WORK = "work";

    private final boolean embedded;
    private final String servers;
    private final int timeOut;
    private final int tickTime;
    private final int initLimit;
    private final int syncLimit;
    private final String dataDir;
    private final String logDataDir;
    private final String rootPath;
    private final int snapRetainCount;
    private final int purgeInterval;

    public ZkConfiguration(boolean embedded, String servers, int timeOut, int tickTime, int initLimit, int syncLimit, String dataDir, String logDataDir, String rootPath, int snapRetainCount, int purgeInterval) {
        this.embedded = embedded;
        this.servers = servers;
        this.timeOut = timeOut;
        this.tickTime = tickTime;
        this.initLimit = initLimit;
        this.syncLimit = syncLimit;
        this.dataDir = dataDir;
        this.logDataDir = logDataDir;
        this.rootPath = rootPath;
        this.snapRetainCount = snapRetainCount;
        this.purgeInterval = purgeInterval;
    }

    public static String buildPath(String... folders) {
        return buildPath(PATH_SEPARATOR, folders);
    }

    static String buildPath(char separator, String... folders) {
        StringBuilder builder = new StringBuilder();
        for (String folder : folders) {
            builder.append(folder);
            builder.append(separator);
        }
        if (builder.length() > 0) {
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String getName(String path) {
        return path.substring(path.lastIndexOf(PATH_SEPARATOR) + 1);
    }

    public static String getParent(String path) {
        if (path.length() == 1) {
            return null;
        }
        String name = getName(path);
        String parent = path.substring(0, path.length() - name.length() - 1);
        if (parent.equals("")) {
            return String.valueOf(PATH_SEPARATOR);
        }
        return parent;
    }

    public boolean isEmbedded() {
        return embedded;
    }

    /**
     * Comma separated list of host ports
     * @return the servers
     */
    public String getServers() {
        return servers;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public int getTickTime() {
        return tickTime;
    }

    public int getInitLimit() {
        return initLimit;
    }

    public int getSyncLimit() {
        return syncLimit;
    }

    public String getDataDir() {
        return dataDir;
    }

    public String getLogDataDir() {
        return logDataDir;
    }

    public String getRootPath() {
        return rootPath;
    }

    public String getNodeQueuePath(String node) {
        return buildPath(getRootPath(), WORK, node + "-queue");
    }

    public String getPath(PathDef pathDef, String... names) {
        if (names.length == 0) {
            return buildPath(getRootPath(), pathDef.getPath(PATH_SEPARATOR));
        }
        String suffixPath = buildPath(names);
        return buildPath(getRootPath(), pathDef.getPath(PATH_SEPARATOR), suffixPath);
    }

    public ZkConfiguration rootPath(String zkRootPath) {
        return new ZkConfiguration(embedded, servers, timeOut, tickTime, initLimit, syncLimit, dataDir, logDataDir, zkRootPath, snapRetainCount, purgeInterval);
    }

    public int getSnapRetainCount() {
        return snapRetainCount;
    }

    public int getPurgeInterval() {
        return purgeInterval;
    }

    public enum PathDef {
        MASTER("current master ephemeral", true, "master"), //
        VERSION("current cluster version", true, "version"), //
        NODES_METADATA("metadata of connected & unconnected nodes", true, NODES, "metadata"), //
        NODES_LIVE("ephemerals of connected nodes", true, NODES, "live"), //
        NODE_METRICS("metrics information of nodes", false, NODES, "metrics"), //
        INDICES_METADATA("metadata of live & error indices", true, "indicies"), //
        SHARD_TO_NODES("ephemerals of nodes serving a shard", true, "shard-to-nodes"), //
        MASTER_QUEUE("master operations", false, WORK, "master-queue"), //
        NODE_QUEUE("node operations and results", false, WORK, "node-queues"), //
        FLAGS("custom flags", false, WORK, "flags"); //

        private final String _description;
        private final String[] _pathParts;
        private final boolean _vip;// very-important-path

        PathDef(String description, boolean vip, String... pathParts) {
            _description = description;
            _vip = vip;
            _pathParts = pathParts;
        }

        public String getPath(char separator) {
            return buildPath(separator, _pathParts);
        }

        public String getDescription() {
            return _description;
        }

        public boolean isVip() {
            return _vip;
        }
    }
}
