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

import com.dasasian.chok.node.NodeContext;

import java.util.List;

public class ShardUndeployOperation extends AbstractShardOperation {

    private static final long serialVersionUID = 1L;

    public ShardUndeployOperation(List<String> shards) {
        for (String shard : shards) {
            addShard(shard);
        }
    }

    @Override
    protected String getOperationName() {
        return "undeploy";
    }

    @Override
    protected void execute(NodeContext context, String shardName, DeployResult deployResult) throws Exception {
        context.getProtocol().unpublishShard(context.getNode(), shardName);
        context.getContentServer().removeShard(shardName);
        context.getShardManager().uninstallShard(shardName);
    }

    @Override
    protected void onException(NodeContext context, String shardName, Exception e) {
        context.getShardManager().uninstallShard(shardName);
    }

}
