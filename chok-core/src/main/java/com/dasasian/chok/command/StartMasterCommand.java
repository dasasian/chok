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

import com.dasasian.chok.master.Master;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.MasterConfigurationLoader;
import com.dasasian.chok.util.ZkChokUtil;
import com.dasasian.chok.util.ZkConfiguration;
import com.google.inject.Inject;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: damith.chandrasekara
 * Date: 7/6/13
 */
public class StartMasterCommand extends Command {

    protected static final Logger LOG = LoggerFactory.getLogger(StartMasterCommand.class);
    private boolean embeddedMode;
    private final ChokFileSystem.Factory chokFileSystemFactory;

    @Inject
    public StartMasterCommand(ChokFileSystem.Factory chokFileSystemFactory) {
        super("startMaster", "[-e] [-ne]", "Starts a local master. -e & -ne for embedded and non-embedded zk-server (overriding configuration)");
        this.chokFileSystemFactory = chokFileSystemFactory;
    }

    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) throws Exception {
        if (optionMap.containsKey("-e")) {
            embeddedMode = true;
        } else if (optionMap.containsKey("-ne")) {
            embeddedMode = false;
        } else {
            embeddedMode = zkConf.isEmbedded();
        }
    }

    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
        final Master master;
        final DatadirCleanupManager datadirCleanupManager;
        if (embeddedMode) {
            LOG.info("starting embedded zookeeper server...");
            ZkServer zkServer = ZkChokUtil.startZkServer(zkConf);
            master = new Master(MasterConfigurationLoader.loadConfiguration(), new InteractionProtocol(zkServer.getZkClient(), zkConf), chokFileSystemFactory, zkServer, false);
            datadirCleanupManager = ZkChokUtil.getDatadirCleanupManager(zkConf);
        } else {
            datadirCleanupManager = null;
            ZkClient zkClient = ZkChokUtil.startZkClient(zkConf, 30000);
            master = new Master(MasterConfigurationLoader.loadConfiguration(), new InteractionProtocol(zkClient, zkConf), chokFileSystemFactory, true);
        }
        master.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                synchronized (master) {
                    if (datadirCleanupManager != null) {
                        datadirCleanupManager.shutdown();
                    }
                    master.shutdown();
                    master.notifyAll();
                }
            }
        });
        synchronized (master) {
            master.wait();
        }
    }

}
