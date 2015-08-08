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
import com.dasasian.chok.protocol.metadata.NodeMetaData;
import com.dasasian.chok.util.ZkConfiguration;

import java.util.Collections;
import java.util.List;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class RemoveNodeCommand extends ProtocolCommand {

    private String nodeName;

    public RemoveNodeCommand() {
        super("removeNode", "<node name>", "Remove a node from Chok");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        CommandLineHelper.validateMinArguments(args, 2);
        nodeName = args[1];
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) {
        if(protocol.getLiveNodes().contains(nodeName)) {
            System.err.println("Cannot remove live node "+nodeName);
        }
        else {
            protocol.unpublishNode(nodeName);
            System.out.println("undeployed node '" + nodeName + "'");
        }
    }
}
