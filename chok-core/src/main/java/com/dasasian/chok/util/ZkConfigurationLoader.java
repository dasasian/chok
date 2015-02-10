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

import com.google.common.base.Optional;

import java.io.File;

public class ZkConfigurationLoader {

    public static final String ZOOKEEPER_EMBEDDED = "zookeeper.embedded";
    public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";
    public static final String ZOOKEEPER_TIMEOUT = "zookeeper.timeout";
    public static final String ZOOKEEPER_TICK_TIME = "zookeeper.tick-time";
    public static final String ZOOKEEPER_INIT_LIMIT = "zookeeper.init-limit";
    public static final String ZOOKEEPER_SYNC_LIMIT = "zookeeper.sync-limit";
    public static final String ZOOKEEPER_DATA_DIR = "zookeeper.data-dir";
    public static final String ZOOKEEPER_LOG_DATA_DIR = "zookeeper.log-data-dir";

    public static final String ZOOKEEPER_ROOT_PATH = "zookeeper.root-path";

    public static ZkConfiguration loadConfiguration() {
        return loadConfiguration(Optional.<Boolean>absent(), Optional.<String>absent(), Optional.<Integer>absent(), Optional.<Integer>absent(), Optional.<Integer>absent(), Optional.<Integer>absent(), Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent());
    }

    public static ZkConfiguration loadConfiguration(Optional<Boolean> overrideEmbedded, Optional<String> overrideServers, Optional<Integer> overrideTimeOut, Optional<Integer> overrideTickTime, Optional<Integer> overrideInitLimit, Optional<Integer> overrideSyncLimit, Optional<String> overrideDataDir, Optional<String> overrideLogDataDir, Optional<String> overrideRootPath) {
        ChokConfiguration chokConfiguration = new ChokConfiguration("/chok.zk.properties");

        boolean embedded = overrideEmbedded.isPresent() ? overrideEmbedded.get() : chokConfiguration.getBoolean(ZOOKEEPER_EMBEDDED, true);

        String servers = overrideServers.isPresent() ? overrideServers.get() : chokConfiguration.getProperty(ZOOKEEPER_SERVERS, "localhost:2181");

        int timeOut = overrideTimeOut.isPresent() ? overrideTimeOut.get() : chokConfiguration.getInt(ZOOKEEPER_TIMEOUT, 1000);

        int tickTime = overrideTickTime.isPresent() ? overrideTickTime.get() : chokConfiguration.getInt(ZOOKEEPER_TICK_TIME, 2000);

        int initLimit = overrideInitLimit.isPresent() ? overrideInitLimit.get() : chokConfiguration.getInt(ZOOKEEPER_INIT_LIMIT, 5);

        int syncLimit = overrideSyncLimit.isPresent() ? overrideSyncLimit.get() : chokConfiguration.getInt(ZOOKEEPER_SYNC_LIMIT, 2);

        String dataDir = overrideDataDir.isPresent() ? overrideDataDir.get() : chokConfiguration.getProperty(ZOOKEEPER_DATA_DIR, "/tmp/zookeeper/data");

        String dataLogDir = overrideLogDataDir.isPresent() ? overrideLogDataDir.get() : chokConfiguration.getProperty(ZOOKEEPER_LOG_DATA_DIR, "/tmp/zookeeper/log");

        String rootPath = overrideRootPath.isPresent() ? overrideRootPath.get() : chokConfiguration.getProperty(ZOOKEEPER_ROOT_PATH, "/chok");

        return new ZkConfiguration(embedded, servers, timeOut, tickTime, initLimit, syncLimit, dataDir, dataLogDir, rootPath);
    }

    public static ZkConfiguration createConfiguration(int port, String zkRootPath, File zkDataDir, File zkLogDataDir) {
        return new ZkConfiguration(true, "localhost:" + port, 1000, 2000, 5, 2, zkDataDir.getAbsolutePath(), zkLogDataDir.getAbsolutePath(), zkRootPath);
    }

    public static ZkConfiguration createConfiguration(String servers, String zkRootPath, File zkDataDir, File zkLogDataDir) {
        return new ZkConfiguration(true, servers, 1000, 2000, 5, 2, zkDataDir.getAbsolutePath(), zkLogDataDir.getAbsolutePath(), zkRootPath);
    }

}
