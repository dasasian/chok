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

import javax.annotation.Nullable;
import java.nio.file.Path;

public class NodeConfigurationLoader {

    private final static String NODE_SERVER_PORT_START = "node.server.port.start";
    private static final String SHARD_FOLDER = "node.shard.folder";
    private static final String SHARD_DEPLOY_THROTTLE = "node.shard.deploy.throttle";
    private static final String SHARD_RELOAD_CHECK_INTERVAL = "node.shard.reload-check-interval";
    private static final String MONITOR_CLASS = "node.monitor.class";
    private static final String RPC_HANDLER_COUNT = "node.rpc.handler-count";

    public static NodeConfiguration loadConfiguration() throws ClassNotFoundException {
        return loadConfiguration(null, null, null, null, null, null);
    }

    public static NodeConfiguration loadConfiguration(int overrideStartPort) throws ClassNotFoundException {
        return loadConfiguration(overrideStartPort, null, null, null, null, null);
    }

    public static NodeConfiguration loadConfiguration(@Nullable Integer overrideStartPort, @Nullable Path overrideShardFolder) throws ClassNotFoundException {
        return loadConfiguration(overrideStartPort, overrideShardFolder, null, null, null, null);
    }

    public static NodeConfiguration loadConfiguration(@Nullable Integer overrideStartPort, @Nullable Path overrideShardFolder, Integer overrideShardDeployThrottle, Class<? extends IMonitor> overrideMonitorClass, Integer overrideRPCHandlerCount, Integer overrideShardReloachCheckInterval) throws ClassNotFoundException {
        ChokConfiguration chokConfiguration = new ChokConfiguration("/chok.node.properties");

        int startPort = overrideStartPort != null ? overrideStartPort : chokConfiguration.getInt(NODE_SERVER_PORT_START);

        Path shardFolder = overrideShardFolder != null ? overrideShardFolder : chokConfiguration.getPath(SHARD_FOLDER);

        int shardDeployThrottle = overrideShardDeployThrottle != null ? overrideShardDeployThrottle : chokConfiguration.getInt(SHARD_DEPLOY_THROTTLE, 0);

        Class<? extends IMonitor> monitorClass = overrideMonitorClass != null ? overrideMonitorClass : ClassUtil.forName(chokConfiguration.getProperty(MONITOR_CLASS, JmxMonitor.class.getName()), IMonitor.class);

        int rpcHandlerCount = overrideRPCHandlerCount != null ? overrideRPCHandlerCount : chokConfiguration.getInt(RPC_HANDLER_COUNT, 25);

        int reloadCheckInterval = overrideShardReloachCheckInterval != null ? overrideShardReloachCheckInterval : chokConfiguration.getInt(SHARD_RELOAD_CHECK_INTERVAL, 25);

        return new NodeConfiguration(startPort, shardFolder, shardDeployThrottle, monitorClass, rpcHandlerCount, reloadCheckInterval);
    }

    public static NodeConfiguration createConfiguration(int startPort, Path shardFolder) {
        return new NodeConfiguration(startPort, shardFolder, 0, JmxMonitor.class, 25, 0);
    }

}
