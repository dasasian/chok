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
import com.dasasian.chok.util.HadoopUtil;
import com.google.common.collect.Lists;
import org.I0Itec.zkclient.ExceptionUtil;
import com.dasasian.chok.util.ChokFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class IndexDeployOperation extends AbstractIndexOperation {

    private static final long serialVersionUID = 1L;
    private final static Logger LOG = LoggerFactory.getLogger(AbstractIndexOperation.class);
    private final String indexName;
    private final String indexPath;
    protected final IndexMetaData indexMetaData;

    public IndexDeployOperation(String indexName, String indexPath, int replicationLevel) {
        indexMetaData = new IndexMetaData(indexName, indexPath, replicationLevel);
        this.indexName = indexName;
        this.indexPath = indexPath;
    }

    protected static List<Shard> readShardsFromFs(final String indexName, final String indexPathString) throws IndexDeployException {
        // get shard folders from source
        URI uri;
        try {
            uri = ChokFileSystem.getURI(indexPathString);
        } catch (final URISyntaxException e) {
            throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE, "unable to parse index path uri '" + indexPathString + "', make sure it starts with file:// or hdfs:// ", e);
        }
        ChokFileSystem fileSystem;
        try {
            fileSystem = HadoopUtil.getChokFileSystem(uri);
        } catch (final IOException e) {
            throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE, "unable to retrieve file system for index path '" + indexPathString + "', make sure your path starts with hadoop support prefix like file:// or hdfs://", e);
        }

        List<Shard> shards = Lists.newArrayList();
        try {
//            final Path indexPath = new Path(indexPathString);
            if (!fileSystem.exists(uri)) {
                throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE, "index path '" + uri + "' does not exists");
            }
            for (final URI subUri : fileSystem.list(uri)) {
                final String shardPath = subUri.getPath();
                if (fileSystem.isDir(subUri) || shardPath.endsWith(".zip")) {
                    shards.add(new Shard(createShardName(indexName, shardPath), shardPath));
                }
            }
        } catch (final URISyntaxException | IOException e) {
            throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE, "could not access index path: " + indexPathString, e);
        }

        if (shards.size() == 0) {
            throw new IndexDeployException(ErrorType.INDEX_NOT_ACCESSIBLE, "index does not contain any shard");
        }
        return shards;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public int getReplicationLevel() {
        return indexMetaData.getReplicationLevel();
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        InteractionProtocol protocol = context.getProtocol();
        try {
            indexMetaData.getShards().addAll(readShardsFromFs(indexName, indexPath));
            LOG.info("Found shards '" + indexMetaData.getShards() + "' for index '" + indexName + "'");
            return distributeIndexShards(context, indexMetaData, protocol.getLiveNodes(), runningOperations);
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
