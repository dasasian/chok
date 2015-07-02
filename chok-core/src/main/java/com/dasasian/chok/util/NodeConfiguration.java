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

import com.dasasian.chok.node.monitor.IMonitor;
import com.dasasian.chok.node.monitor.JmxMonitor;

import java.io.File;
import java.nio.file.Path;

public class NodeConfiguration {

    private final int startPort;
    private final Path shardFolder;
    private final int shardDeployThrottle;
    private final Class<? extends IMonitor> monitorClass;
    private final int rpcHandlerCount;

    public NodeConfiguration(int startPort, Path shardFolder) {
        this(startPort, shardFolder, 0, JmxMonitor.class, 25);
    }

    public NodeConfiguration(int startPort, Path shardFolder, int shardDeployThrottle, Class<? extends IMonitor> monitorClass, int rpcHandlerCount) {
        this.startPort = startPort;
        this.shardFolder = shardFolder;
        this.shardDeployThrottle = shardDeployThrottle;
        this.monitorClass = monitorClass;
        this.rpcHandlerCount = rpcHandlerCount;
    }

    public int getStartPort() {
        return startPort;
    }

    public Path getShardFolder() {
        return shardFolder;
    }

    /**
     * @return a bandwidth limitation in bytes/sec for shard installation
     */
    public int getShardDeployThrottle() {
        return shardDeployThrottle;
    }

    public Class<? extends IMonitor> getMonitorClass() {
        return monitorClass;
    }

    public int getRpcHandlerCount() {
        return rpcHandlerCount;
    }

}

