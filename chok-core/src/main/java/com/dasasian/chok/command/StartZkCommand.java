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
package com.dasasian.chok.command;

import com.dasasian.chok.util.ZkChokUtil;
import com.dasasian.chok.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkServer;
import org.apache.zookeeper.server.DatadirCleanupManager;

/**
 * User: damith.chandrasekara
 * Date: 7/6/13
 */
public class StartZkCommand extends Command {

    public StartZkCommand() {
        super("startZk", "", "Starts a local zookeeper server");
    }

    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
        final ZkServer zkServer = ZkChokUtil.startZkServer(zkConf);
        DatadirCleanupManager datadirCleanupManager = ZkChokUtil.getDatadirCleanupManager(zkConf);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                synchronized (zkServer) {
                    System.out.println("stopping zookeeper datadir cleanup...");
                    datadirCleanupManager.shutdown();
                    System.out.println("stopping zookeeper server...");
                    zkServer.shutdown();
                    zkServer.notifyAll();
                }
            }
        });
        System.out.println("zookeeper server started on port " + zkServer.getPort());
        zkServer.wait();
    }

}
