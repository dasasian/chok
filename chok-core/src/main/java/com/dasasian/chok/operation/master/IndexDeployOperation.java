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
package com.dasasian.chok.operation.master;

import com.dasasian.chok.master.MasterContext;
import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexDeployError.ErrorType;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.dasasian.chok.util.ChokFileSystem;
import com.google.common.collect.Lists;
import org.I0Itec.zkclient.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class IndexDeployOperation extends AbstractIndexOperation {

    private static final long serialVersionUID = 1L;
    private final static Logger LOG = LoggerFactory.getLogger(AbstractIndexOperation.class);
    private final String indexName;
    private final URI indexUri;
    protected final IndexMetaData indexMetaData;

    public IndexDeployOperation(String indexName, URI indexUri, int replicationLevel) {
        this(indexName, indexUri, replicationLevel, false);
    }

    public IndexDeployOperation(String indexName, URI indexUri, int replicationLevel, boolean autoReload) {
        indexMetaData = new IndexMetaData(indexName, indexUri, replicationLevel, autoReload);
        this.indexName = indexName;
        this.indexUri = indexUri;
    }

    protected static List<Shard> readShardsFromFs(final ChokFileSystem fileSystem, final String indexName, final URI indexUri) throws IndexDeployException {
        List<Shard> shards = Lists.newArrayList();
        try {
            for (final URI shardUri : fileSystem.list(indexUri)) {
                if (fileSystem.isDir(shardUri) || shardUri.toString().endsWith(".zip")) {
                    shards.add(new Shard(createShardName(indexName, shardUri.getPath()), shardUri));
                }
            }
        } catch (final URISyntaxException | IOException e) {
            throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE, "could not access index: " + indexUri, e);
        }

        if (shards.size() == 0) {
            throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE, "index does not contain any shard");
        }
        return shards;
    }

    public String getIndexName() {
        return indexName;
    }

    public int getReplicationLevel() {
        return indexMetaData.getReplicationLevel();
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        try {
            try {
                ChokFileSystem fileSystem = context.getChokFileSystem(indexMetaData);
                indexMetaData.getShards().addAll(readShardsFromFs(fileSystem, indexName, indexUri));
                LOG.info("Found shards '" + indexMetaData.getShards() + "' for index '" + indexName + "'");
                return distributeIndexShards(context, indexMetaData, runningOperations);
            } catch (final IOException e) {
                throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE, "unable to retrieve file system for index path '" + indexUri + "', make sure your path starts with hadoop support prefix like file:// or hdfs://", e);
            }

        } catch (Exception e) {
            ExceptionUtil.rethrowInterruptedException(e);
            LOG.error("failed to deploy index " + indexName, e);
            // note: need to publishIndex before update can be done to the indexMD
            protocol.publishIndex(indexMetaData);
            handleMasterDeployException(protocol, indexMetaData, e);
            return null;
        }
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
        LOG.info("deployment of index " + indexName + " complete");
        handleDeploymentComplete(context, results, indexMetaData, true);
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        for (MasterOperation operation : runningOperations) {
            if (operation instanceof IndexDeployOperation && ((IndexDeployOperation) operation).indexName.equals(indexName)) {
                return ExecutionInstruction.CANCEL;
            }
        }
        return ExecutionInstruction.EXECUTE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + indexName;
    }

}
