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
import com.dasasian.chok.node.ShardManager;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;

public class ShardReloadOperation extends AbstractShardOperation {

    private static final long serialVersionUID = 1L;

    public ShardReloadOperation(Map<String, URI> reloadShards) {
        reloadShards.forEach(this::addShard);
    }

    @Override
    protected String getOperationName() {
        return "reload";
    }

    @Override
    protected void execute(NodeContext context, String shardName, DeployResult deployResult) throws Exception {
        URI shardUri = getShardUri(shardName);
        // auto reload is set to false since we would only reload a shard if this was alread set
        // so no need to do work twice
        final ShardManager shardManager = context.getShardManager();
        final IContentServer contentServer = context.getContentServer();

        Path localShardFolder = shardManager.installShard(shardName, shardUri, false);

        Path replacedLocalShardFolder = contentServer.replaceShard(shardName, localShardFolder);

        shardManager.removeLocalShardFolder(replacedLocalShardFolder);

        Map<String, String> shardMetaData = context.getContentServer().getShardMetaData(shardName);
        if (shardMetaData == null) {
            throw new IllegalStateException("node managed '" + context.getContentServer() + "' does return NULL as shard metadata");
        }
        deployResult.addShardMetaDataMap(shardName, shardMetaData);

        publishShard(shardName, context);
    }

    @Override
    protected void onException(NodeContext context, String shardName, Exception e) {
        context.getShardManager().uninstallShard(shardName);
    }

}
