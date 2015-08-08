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
package com.dasasian.chok.client;

import com.dasasian.chok.operation.master.AbstractIndexOperation;
import com.dasasian.chok.operation.master.IndexDeployOperation;
import com.dasasian.chok.operation.master.IndexUndeployOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexMetaData;

import java.net.URI;
import java.util.List;

public class DeployClient implements IDeployClient {

    private final InteractionProtocol protocol;

    public DeployClient(InteractionProtocol interactionProtocol) {
        protocol = interactionProtocol;
    }

    @Override
    public IIndexDeployFuture addIndex(String indexName, URI indexUri, int replicationLevel, boolean autoReload) {
        validateIndexData(indexName, replicationLevel);
        protocol.addMasterOperation(new IndexDeployOperation(indexName, indexUri, replicationLevel, autoReload));
        return new IndexDeployFuture(protocol, indexName);
    }

    private void validateIndexData(String name, int replicationLevel) {
        // TODO jz: try to access path already ?
        if (replicationLevel <= 0) {
            throw new IllegalArgumentException("replication level must be 1 or greater");
        }
        if (name.trim().equals("*")) {
            throw new IllegalArgumentException("invalid index name: " + name);
        }
        if (name.contains(AbstractIndexOperation.INDEX_SHARD_NAME_SEPARATOR + "")) {
            throw new IllegalArgumentException("invalid index name : " + name + " - must not contain " + AbstractIndexOperation.INDEX_SHARD_NAME_SEPARATOR);
        }
    }

    @Override
    public void removeIndex(String indexName) {
        if (!existsIndex(indexName)) {
            throw new IllegalArgumentException("index with name '" + indexName + "' does not exists");
        }
        protocol.addMasterOperation(new IndexUndeployOperation(indexName));
    }

    @Override
    public boolean existsIndex(String indexName) {
        return protocol.indexExists(indexName);
    }

    @Override
    public IndexMetaData getIndexMetaData(String indexName) {
        return protocol.getIndexMD(indexName);
    }

    @Override
    public List<String> getIndices() {
        return protocol.getIndices();
    }
}
