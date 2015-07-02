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
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.util.ZkConfiguration;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class RedeployIndexCommand extends ProtocolCommand {

    private String indexName;

    public RedeployIndexCommand() {
        super("redeployIndex", "<index name>", "Undeploys and deploys an index");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        CommandLineHelper.validateMinArguments(args, 2);
        indexName = args[1];
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        IndexMetaData indexMD = protocol.getIndexMD(indexName);
        if (indexMD == null) {
            throw new IllegalArgumentException("index '" + indexName + "' does not exist");
        }
        CommandLineHelper.removeIndex(protocol, indexName);
        Thread.sleep(5000);
        CommandLineHelper.addIndex(protocol, indexName, indexMD.getUri(), indexMD.getReplicationLevel());
    }
}
