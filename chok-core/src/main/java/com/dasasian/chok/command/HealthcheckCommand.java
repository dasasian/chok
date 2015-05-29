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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class HealthcheckCommand extends ProtocolCommand {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private boolean prettyView;

    public HealthcheckCommand() {
        super("healthcheck", "[-p]", "Shows the status of a Chok installation. -p for pretty view.");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        prettyView = optionMap.containsKey("-p");
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) {
        if(prettyView) {
            OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        }

        Map<String, Map<String, Object>> healthchecks = Maps.newHashMap();

        Map<String, Object> nodesHealthcheckResult = Maps.newHashMap();
        final int knownNodesCount = protocol.getKnownNodes().size();
        final int liveNodesCount = protocol.getLiveNodes().size();
        nodesHealthcheckResult.put("healthy", (liveNodesCount == knownNodesCount));
        nodesHealthcheckResult.put("message", "Nodes (active/known): " + liveNodesCount + "/" + knownNodesCount);
        healthchecks.put("nodes", nodesHealthcheckResult);

        Map<String, Object> indicesHealthcheckResult = Maps.newHashMap();
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
        indicesHealthcheckResult.put("healthy", ((indices.size() == deployedCount) && (deployedCount == balancedCount)));
        indicesHealthcheckResult.put("message", "Indices (deployed/balanced/known): " + deployedCount + "/" + balancedCount + "/" + indices.size());
        healthchecks.put("indices", indicesHealthcheckResult);

        try {
            System.out.println(OBJECT_MAPPER.writeValueAsString(healthchecks));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }
}
