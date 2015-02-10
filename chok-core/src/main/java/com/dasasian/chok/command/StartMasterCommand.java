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
import com.dasasian.chok.util.MasterConfigurationLoader;
import com.dasasian.chok.util.ZkChokUtil;
import com.dasasian.chok.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;

/**
 * User: damith.chandrasekara
 * Date: 7/6/13
 */
public class StartMasterCommand extends Command {

    protected static final Logger LOG = Logger.getLogger(StartMasterCommand.class);
    private boolean _embeddedMode;

    public StartMasterCommand() {
        super("startMaster", "[-e] [-ne]", "Starts a local master. -e & -ne for embedded and non-embedded zk-server (overriding configuration)");
    }

    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) throws Exception {
        if (optionMap.containsKey("-e")) {
            _embeddedMode = true;
        } else if (optionMap.containsKey("-ne")) {
            _embeddedMode = false;
        } else {
            _embeddedMode = zkConf.isEmbedded();
        }
    }

    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
        final Master master;
        if (_embeddedMode) {
            LOG.info("starting embedded zookeeper server...");
            ZkServer zkServer = ZkChokUtil.startZkServer(zkConf);
            master = new Master(MasterConfigurationLoader.loadConfiguration(), new InteractionProtocol(zkServer.getZkClient(), zkConf), zkServer, false);
        } else {
            ZkClient zkClient = ZkChokUtil.startZkClient(zkConf, 30000);
            master = new Master(MasterConfigurationLoader.loadConfiguration(), new InteractionProtocol(zkClient, zkConf), true);
        }
        master.start();

        synchronized (master) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    synchronized (master) {
                        master.shutdown();
                        master.notifyAll();
                    }
                }
            });
            master.wait();
        }
    }
}
