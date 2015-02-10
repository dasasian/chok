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
import com.google.common.base.Optional;

import java.io.File;

public class NodeConfigurationLoader {

    private final static String NODE_SERVER_PORT_START = "node.server.port.start";
    private static final String SHARD_FOLDER = "node.shard.folder";
    private static final String SHARD_DEPLOY_THROTTLE = "node.shard.deploy.throttle";
    private static final String MONITOR_CLASS = "node.monitor.class";
    private static final String RPC_HANDLER_COUNT = "node.rpc.handler-count";

    public static NodeConfiguration loadConfiguration() throws ClassNotFoundException {
        return loadConfiguration(Optional.<Integer>absent(), Optional.<File>absent(), Optional.<Integer>absent(), Optional.<Class<? extends IMonitor>>absent(), Optional.<Integer>absent());
    }

    public static NodeConfiguration loadConfiguration(int overrideStartPort) throws ClassNotFoundException {
        return loadConfiguration(Optional.of(overrideStartPort), Optional.<File>absent(), Optional.<Integer>absent(), Optional.<Class<? extends IMonitor>>absent(), Optional.<Integer>absent());
    }

    public static NodeConfiguration loadConfiguration(Optional<Integer> overrideStartPort, Optional<File> overrideShardFolder) throws ClassNotFoundException {
        return loadConfiguration(overrideStartPort, overrideShardFolder, Optional.<Integer>absent(), Optional.<Class<? extends IMonitor>>absent(), Optional.<Integer>absent());
    }

    public static NodeConfiguration loadConfiguration(Optional<Integer> overrideStartPort, Optional<File> overrideShardFolder, Optional<Integer> overrideShardDeployThrottle, Optional<Class<? extends IMonitor>> overrideMonitorClass, Optional<Integer> overrideRPCHandlerCount) throws ClassNotFoundException {
        ChokConfiguration chokConfiguration = new ChokConfiguration("/chok.node.properties");

        int startPort = overrideStartPort.isPresent() ? overrideStartPort.get() : chokConfiguration.getInt(NODE_SERVER_PORT_START);

        File shardFolder = overrideShardFolder.isPresent() ? overrideShardFolder.get() : chokConfiguration.getFile(SHARD_FOLDER);

        int shardDeployThrottle = overrideShardDeployThrottle.isPresent() ? overrideShardDeployThrottle.get() : chokConfiguration.getInt(SHARD_DEPLOY_THROTTLE, 0);

        Class<? extends IMonitor> monitorClass = overrideMonitorClass.isPresent() ? overrideMonitorClass.get() : ClassUtil.forName(chokConfiguration.getProperty(MONITOR_CLASS, JmxMonitor.class.getName()), IMonitor.class);

        int rpcHandlerCount = overrideRPCHandlerCount.isPresent() ? overrideRPCHandlerCount.get() : chokConfiguration.getInt(RPC_HANDLER_COUNT, 25);

        return new NodeConfiguration(startPort, shardFolder, shardDeployThrottle, monitorClass, rpcHandlerCount);
    }

    public static NodeConfiguration createConfiguration(int startPort, File shardFolder) {
        return new NodeConfiguration(startPort, shardFolder, 0, JmxMonitor.class, 25);
    }

}
