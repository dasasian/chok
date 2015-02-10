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
import com.dasasian.chok.protocol.metadata.IndexDeployError;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.util.ZkConfiguration;

import java.util.List;
import java.util.Set;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class ListErrorsCommand extends ProtocolCommand {

    public ListErrorsCommand() {
        super("listErrors", "<index name>", "Lists all deploy errors for a specified index");
    }

    private String _indexName;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        CommandLineHelper.validateMinArguments(args, 2);
        _indexName = args[1];
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        IndexMetaData indexMD = protocol.getIndexMD(_indexName);
        if (indexMD == null) {
            throw new IllegalArgumentException("index '" + _indexName + "' does not exist");
        }
        if (!indexMD.hasDeployError()) {
            System.out.println("No error for index '" + _indexName + "'");
            return;
        }
        IndexDeployError deployError = indexMD.getDeployError();
        System.out.println("Error Type: " + deployError.getErrorType());
        if (deployError.getErrorMessage() != null) {
            System.out.println("Error Message: " + deployError.getErrorMessage());
        }
        System.out.println("List of shard-errors:");
        Set<IndexMetaData.Shard> shards = indexMD.getShards();
        for (IndexMetaData.Shard shard : shards) {
            List<Exception> shardErrors = deployError.getShardErrors(shard.getName());
            if (shardErrors != null && !shardErrors.isEmpty()) {
                System.out.println("\t" + shard.getName() + ": ");
                for (Exception errorDetail : shardErrors) {
                    System.out.println("\t\t" + errorDetail.getMessage());
                }
            }
        }
    }

}
