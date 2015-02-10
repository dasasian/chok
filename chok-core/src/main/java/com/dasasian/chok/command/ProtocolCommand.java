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

import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;

/**
 * User: damith.chandrasekara
 * Date: 7/6/13
 */
public abstract class ProtocolCommand extends Command {

    public ProtocolCommand(String command, String parameterString, String description) {
        super(command, parameterString, description);
    }

    @Override
    public final void execute(ZkConfiguration zkConf) throws Exception {
        ZkClient zkClient = new ZkClient(zkConf.getServers());
        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
        execute(zkConf, protocol);
        protocol.disconnect();
    }

    protected abstract void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception;
}
