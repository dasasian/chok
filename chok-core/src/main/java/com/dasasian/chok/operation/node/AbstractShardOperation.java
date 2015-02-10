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
import org.I0Itec.zkclient.ExceptionUtil;
import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public abstract class AbstractShardOperation implements NodeOperation {

    private static final long serialVersionUID = 1L;
    private final static Logger LOG = Logger.getLogger(AbstractShardOperation.class);

    private Map<String, String> shardPathsByShardNames = new LinkedHashMap<>(3);

    public Set<String> getShardNames() {
        return shardPathsByShardNames.keySet();
    }

    public String getShardPath(String shardName) {
        return shardPathsByShardNames.get(shardName);
    }

    public void addShard(String shardName, String shardPath) {
        shardPathsByShardNames.put(shardName, shardPath);
    }

    public void addShard(String shardName) {
        shardPathsByShardNames.put(shardName, null);
    }

    @Override
    public final DeployResult execute(NodeContext context) throws InterruptedException {
        DeployResult result = new DeployResult(context.getNode().getName());
        for (String shardName : getShardNames()) {
            try {
                LOG.info(getOperationName() + " shard '" + shardName + "'");
                execute(context, shardName, result);
            } catch (Exception e) {
                ExceptionUtil.rethrowInterruptedException(e);
                LOG.error("failed to " + getOperationName() + " shard '" + shardName + "' on node '" + context.getNode().getName() + "'", e);
                result.addShardException(shardName, e);
                onException(context, shardName, e);
            }
        }
        return result;
    }

    protected abstract String getOperationName();

    protected abstract void execute(NodeContext context, String shardName, DeployResult result) throws Exception;

    protected abstract void onException(NodeContext context, String shardName, Exception e);

    protected void publishShard(String shardName, NodeContext context) {
        LOG.info("publish shard '" + shardName + "'");
        context.getProtocol().publishShard(context.getNode(), shardName);
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + getShardNames();
    }

}
