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
package com.dasasian.chok.operation.node;

import com.dasasian.chok.node.IContentServer;
import com.dasasian.chok.node.NodeContext;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Redeploys shards which are already installed in {@link com.dasasian.chok.node.ShardManager}.
 */
public class ShardRedeployOperation extends AbstractShardOperation {

    private static final long serialVersionUID = 1L;

    public ShardRedeployOperation(Map<String, URI> installedShards) {
        installedShards.forEach(this::addShard);
    }

    @Override
    protected String getOperationName() {
        return "redeploy";
    }

    @Override
    protected void execute(NodeContext context, String shardName, DeployResult deployResult) throws Exception {
        Path localShardFolder = Paths.get(getShardUri(shardName));
        IContentServer contentServer = context.getContentServer();
        if (!contentServer.getShards().contains(shardName)) {
            contentServer.addShard(shardName, localShardFolder);
        }
        publishShard(shardName, context);
    }

    @Override
    protected void onException(NodeContext context, String shardName, Exception e) {
        context.getShardManager().uninstallShard(shardName);
    }

}
