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

import java.net.URI;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class AddIndexCommand extends ProtocolCommand {

    private String name;
    private String uriString;
    private int replicationLevel = 3;
    private boolean autoReload = false;

    public AddIndexCommand() {
        super("addIndex", "<index name> <uri to index> [<replication level>] [<auto reload (true/false)>]", "Add a index to Chok");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        CommandLineHelper.validateMinArguments(args, 3);
        name = args[1];
        uriString = args[2];
        if (args.length >= 4) {
            replicationLevel = Integer.parseInt(args[3]);
        }
        if (args.length >= 5) {
            autoReload = Boolean.parseBoolean(args[4].trim());
        }
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        CommandLineHelper.addIndex(protocol, name, new URI(uriString), replicationLevel, autoReload);
    }

}
