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

import com.dasasian.chok.protocol.ChokZkSerializer;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.server.DatadirCleanupManager;

public class ZkChokUtil {

    public static final Splitter COMMA_SPLITTER = Splitter.on(",");

    public static ZkClient startZkClient(ZkConfiguration conf) {
        return startZkClient(conf, Integer.MAX_VALUE);
    }

    public static ZkClient startZkClient(ZkConfiguration conf, int connectionTimeout) {
        return startZkClient(conf.getServers(), conf.getTimeOut(), connectionTimeout);
    }

    public static ZkClient startZkClient(String servers) {
        return startZkClient(new ZkConnection(servers), Integer.MAX_VALUE, new ChokZkSerializer());
    }

    private static ZkClient startZkClient(String servers, int sessionTimeout, int connectionTimeout) {
        return startZkClient(servers, sessionTimeout, connectionTimeout, new ChokZkSerializer());
    }


    public static ZkClient startZkClient(String servers, int sessionTimeout, int connectionTimeout, ZkSerializer zkSerializer) {
        return startZkClient(new ZkConnection(servers, sessionTimeout), connectionTimeout, zkSerializer);
    }

    public static ZkClient startZkClient(IZkConnection iZkConnection, int connectionTimeout, ZkSerializer zkSerializer) {
        return new ZkClient(iZkConnection, connectionTimeout, zkSerializer);
    }

    public static ZkServer startZkServer(ZkConfiguration conf) {
        String server = Iterables.getOnlyElement(COMMA_SPLITTER.split(conf.getServers()));
        HostAndPort hostAndPort = HostAndPort.fromString(server);
        if (!hostAndPort.hasPort()) {
            throw new IllegalArgumentException("No Port Specified for ZkServer");
        }
        ZkServer zkServer = new ZkServer(conf.getDataDir(), conf.getLogDataDir(), new DefaultNameSpaceImpl(conf), hostAndPort.getPort(), conf.getTickTime());
        zkServer.start();
        zkServer.getZkClient().setZkSerializer(new ChokZkSerializer());
        return zkServer;
    }

    public static DatadirCleanupManager getDatadirCleanupManager(ZkConfiguration conf) {
        DatadirCleanupManager datadirCleanupManager;
        if(conf.getPurgeInterval()>0) {
            datadirCleanupManager = new DatadirCleanupManager(conf.getDataDir(), conf.getLogDataDir(), conf.getSnapRetainCount(), conf.getPurgeInterval());
            datadirCleanupManager.start();
        }
        else {
            datadirCleanupManager = null;
        }
        return datadirCleanupManager;
    }

}
