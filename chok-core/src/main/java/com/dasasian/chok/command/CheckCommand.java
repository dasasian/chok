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

import com.dasasian.chok.client.IndexState;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.ReplicationReport;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.util.ZkConfiguration;

import java.util.*;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class CheckCommand extends ProtocolCommand {

    private boolean _batchMode;
    private boolean _skipColumnNames;
    private boolean _sorted;
    public CheckCommand() {
        super("check", "[-b] [-n] [-S]", "Analyze index/shard/node status. -b for batch mode, -n don't write column names, -S for sorting the index/shard/node names.");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        _batchMode = optionMap.containsKey("-b");
        _skipColumnNames = optionMap.containsKey("-n");
        _sorted = optionMap.containsKey("-S");
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("            Index Analysis");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        List<String> indices = protocol.getIndices();
        if (_sorted) {
            Collections.sort(indices);
        }
        CommandLineHelper.CounterMap<IndexState> indexStateCounterMap = new CommandLineHelper.CounterMap<>();
        for (String index : indices) {
            IndexMetaData indexMD = protocol.getIndexMD(index);
            if (indexMD.hasDeployError()) {
                indexStateCounterMap.increment(IndexState.ERROR);
            } else {
                indexStateCounterMap.increment(IndexState.DEPLOYED);
            }
        }
        CommandLineHelper.Table tableIndexStates = new CommandLineHelper.Table("Index State", "Count");
        tableIndexStates.setBatchMode(_batchMode);
        tableIndexStates.setSkipColumnNames(_skipColumnNames);
        List<IndexState> keySet = new ArrayList<>(indexStateCounterMap.keySet());
        if (_sorted) {
            Collections.sort(keySet);
        }
        for (IndexState indexState : keySet) {
            tableIndexStates.addRow(indexState, indexStateCounterMap.getCount(indexState));
        }
        System.out.println(tableIndexStates.toString());
        printResume("indices", indices.size(), indexStateCounterMap.getCount(IndexState.DEPLOYED), "deployed");

        System.out.println("\n");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("            Shard Analysis");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
        int totalShards = 0;
        for (String index : indices) {
            System.out.println("checking " + index + " ...");
            IndexMetaData indexMD = protocol.getIndexMD(index);
            ReplicationReport replicationReport = protocol.getReplicationReport(indexMD);
            Set<IndexMetaData.Shard> shards = indexMD.getShards();
            // cannot sort shards because Shard is declared inside IndexMetaData
            totalShards += shards.size() * indexMD.getReplicationLevel();
            for (IndexMetaData.Shard shard : shards) {
                int shardReplication = replicationReport.getReplicationCount(shard.getName());
                if (shardReplication < indexMD.getReplicationLevel()) {
                    System.out.println("\tshard " + shard + " is under-replicated (" + shardReplication + "/" + indexMD.getReplicationLevel() + ")");
                } else if (shardReplication > indexMD.getReplicationLevel()) {
                    System.out.println("\tshard " + shard + " is over-replicated (" + shardReplication + "/" + indexMD.getReplicationLevel() + ")");
                }
            }
        }

        long startTime = Long.MAX_VALUE;
        List<String> knownNodes = protocol.getKnownNodes();
        List<String> connectedNodes = protocol.getLiveNodes();
        CommandLineHelper.Table tableNodeLoad = new CommandLineHelper.Table("Node", "Connected", "Shard Status");
        tableNodeLoad.setBatchMode(_batchMode);
        tableNodeLoad.setSkipColumnNames(_skipColumnNames);
        if (_sorted) {
            Collections.sort(knownNodes);
        }
        int publishedShards = 0;
        for (String node : knownNodes) {
            boolean isConnected = connectedNodes.contains(node);
            int shardCount = 0;
            int announcedShardCount = 0;
            for (String shard : protocol.getNodeShards(node)) {
                shardCount++;
                long ctime = protocol.getShardAnnounceTime(node, shard);
                if (ctime > 0) {
                    announcedShardCount++;
                    if (ctime < startTime) {
                        startTime = ctime;
                    }
                }
            }
            publishedShards += announcedShardCount;
            StringBuilder builder = new StringBuilder();
            builder.append(String.format(" %9s ", String.format("%d/%d", announcedShardCount, shardCount)));
            for (int i = 0; i < shardCount; i++) {
                builder.append(i < announcedShardCount ? "#" : "-");
            }
            tableNodeLoad.addRow(node, Boolean.toString(isConnected), builder);
        }
        System.out.println();
        printResume("shards", totalShards, publishedShards, "deployed");
        if (startTime < Long.MAX_VALUE && totalShards > 0 && publishedShards > 0 && publishedShards < totalShards) {
            long elapsed = System.currentTimeMillis() - startTime;
            double timePerShard = (double) elapsed / (double) publishedShards;
            long remaining = Math.round(timePerShard * (totalShards - publishedShards));
            Date finished = new Date(System.currentTimeMillis() + remaining);
            remaining /= 1000;
            long secs = remaining % 60;
            remaining /= 60;
            long min = remaining % 60;
            remaining /= 60;
            System.out.printf("Estimated completion: %s (%dh %dm %ds)", finished, remaining, min, secs);
        }

        System.out.println("\n\n");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("            Node Analysis");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        System.out.println(tableNodeLoad);
        printResume("nodes", knownNodes.size(), connectedNodes.size(), "connected");
    }

    private void printResume(String name, int maximum, int num, String action) {
        double progress = maximum == 0 ? 0.0 : (double) num / (double) maximum;
        System.out.printf("%d out of %d " + name + " " + action + " (%.2f%%)\n", num, maximum, 100 * progress);
    }

}
