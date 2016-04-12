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

import com.dasasian.chok.node.IContentServer;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.ReplicationReport;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.util.ZkConfiguration;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class ListIndicesCommand extends ProtocolCommand {

    private boolean detailedView;
    private boolean batchMode;
    private boolean skipColumnNames;
    private boolean sorted;
    public ListIndicesCommand() {
        super("listIndices", "[-d] [-b] [-n] [-S]", "Lists all indices. -d for detailed view, -b for batch mode, -n don't write column headers, -S for sorting the shard names.");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        detailedView = optionMap.containsKey("-d");
        batchMode = optionMap.containsKey("-b");
        skipColumnNames = optionMap.containsKey("-n");
        sorted = optionMap.containsKey("-S");
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) {
        final CommandLineHelper.Table table;
        if (!detailedView) {
            table = new CommandLineHelper.Table("Name", "Status", "Replication State", "Path", "Shards", "Entries", "Disk Usage");
        } else {
            table = new CommandLineHelper.Table("Name", "Status", "Replication State", "Path", "Shards", "Entries", "Disk Usage", "Replication Count");
        }
        table.setBatchMode(batchMode);
        table.setSkipColumnNames(skipColumnNames);

        List<String> indices = protocol.getIndices();
        if (sorted) {
            Collections.sort(indices);
        }
        for (final String index : indices) {
            final IndexMetaData indexMD = protocol.getIndexMD(index);
            Set<IndexMetaData.Shard> shards = indexMD.getShards();
            String entries = "n/a";
            String indexBytes = "n/a";
            if (!indexMD.hasDeployError()) {
                entries = "" + calculateIndexEntries(shards);
                indexBytes = "" + calculateIndexDiskUsage(shards);
            }
            String state = "DEPLOYED";
            String replicationState = "BALANCED";
            if (indexMD.hasDeployError()) {
                state = "ERROR";
                replicationState = "-";
            } else {
                ReplicationReport report = protocol.getReplicationReport(indexMD, protocol.getLiveNodeCount());
                if (report.isUnderreplicated()) {
                    replicationState = "UNDERREPLICATED";
                } else if (report.isOverreplicated()) {
                    replicationState = "OVERREPLICATED";
                }

            }
            if (!detailedView) {
                table.addRow(index, state, replicationState, indexMD.getUri(), shards.size(), entries, indexBytes);
            } else {
                table.addRow(index, state, replicationState, indexMD.getUri(), shards.size(), entries, indexBytes, indexMD.getReplicationLevel());
            }
        }
        if (!indices.isEmpty()) {
            System.out.println(table.toString());
        }
        if (!batchMode) {
            System.out.println(indices.size() + " registered indices");
            System.out.println();
        }
    }

    private int calculateIndexEntries(Set<IndexMetaData.Shard> shards) {
        int docCount = 0;
        for (IndexMetaData.Shard shard : shards) {
            Map<String, String> metaData = shard.getMetaDataMap();
            if (metaData != null) {
                try {
                    docCount += Integer.parseInt(metaData.get(IContentServer.SHARD_SIZE_KEY));
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
        return docCount;
    }

    private long calculateIndexDiskUsage(Set<IndexMetaData.Shard> shards) {
        long diskUsage = 0;
        for (IndexMetaData.Shard shard : shards) {
            Map<String, String> metaData = shard.getMetaDataMap();
            if (metaData != null) {
                try {
                    diskUsage += Long.parseLong(metaData.get(IContentServer.SHARD_DISK_USAGE_KEY));
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
        return diskUsage;
    }
}
