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
import com.dasasian.chok.protocol.ReplicationReport;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.util.ZkConfiguration;

import java.util.List;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class StatusCommand extends ProtocolCommand {

    // todo
    private boolean detailedView;

    public StatusCommand() {
        super("status", "[-d]", "Shows the status of a Chok installation. -d for detailed view.");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        detailedView = optionMap.containsKey("-d");
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) {

        final int knownNodesCount = protocol.getKnownNodes().size();
        final int liveNodesCount = protocol.getLiveNodes().size();
        final boolean nodesHealthy = liveNodesCount == knownNodesCount;

        List<String> indices = protocol.getIndices();
        int deployedCount = 0;
        int balancedCount = 0;
        for (final String index : indices) {
            final IndexMetaData indexMD = protocol.getIndexMD(index);
            if (!indexMD.hasDeployError()) {
                deployedCount++;
                ReplicationReport report = protocol.getReplicationReport(indexMD);
                if (report.isBalanced()) {
                    balancedCount++;
                }
            }
        }
        final boolean indicesHealthy = (indices.size() == deployedCount) && (deployedCount == balancedCount);

        final boolean healthy = nodesHealthy && indicesHealthy;
        System.out.println("Healthy: "+healthy);
        System.out.println("Nodes (active/known): " + liveNodesCount + "/" + knownNodesCount);
        System.out.println("Indices (deployed/balanced/known): " + deployedCount + "/" + balancedCount + "/" + indices.size());

        boolean isIndexAutoRepairEnabled = protocol.isIndexAutoRepairEnabled();
        System.out.println("Index Auto Repair: " + (isIndexAutoRepairEnabled ? "enabled" : "disabled"));
    }
}
