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
public class ListNodesCommand extends ProtocolCommand {

    private boolean _batchMode;
    private boolean _skipColumnNames;
    private boolean _sorted;
    public ListNodesCommand() {
        super("listNodes", "[-d] [-b] [-n] [-S]", "Lists all nodes. -b for batch mode, -n don't write column headers, -S for sorting the node names.");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        _batchMode = optionMap.containsKey("-b");
        _skipColumnNames = optionMap.containsKey("-n");
        _sorted = optionMap.containsKey("-S");
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) {
        final List<String> knownNodes = protocol.getKnownNodes();
        final List<String> liveNodes = protocol.getLiveNodes();
        final CommandLineHelper.Table table = new CommandLineHelper.Table();
        table.setBatchMode(_batchMode);
        table.setSkipColumnNames(_skipColumnNames);
        if (_sorted) {
            Collections.sort(knownNodes);
        }
        for (final String node : knownNodes) {
            NodeMetaData nodeMetaData = protocol.getNodeMD(node);
            table.addRow(nodeMetaData.getName(), nodeMetaData.getStartTimeAsDate(), liveNodes.contains(node) ? "CONNECTED" : "DISCONNECTED");
        }
        table.setHeader("Name (" + liveNodes.size() + "/" + knownNodes.size() + " connected)", "Start time", "State");
        System.out.println(table.toString());
    }
}
