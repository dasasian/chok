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
package com.dasasian.chok.mapfile.testutil;

import com.dasasian.chok.mapfile.IMapFileClient;
import com.dasasian.chok.mapfile.MapFileClient;
import com.dasasian.chok.node.NodeContext;
import com.dasasian.chok.testutil.loadtest.query.AbstractQueryExecutor;
import com.dasasian.chok.util.ZkConfiguration;

@SuppressWarnings("serial")
public class MapfileAccessExecutor extends AbstractQueryExecutor {

    private final ZkConfiguration _zkConfOfTargetCluster;
    private IMapFileClient _client;

    public MapfileAccessExecutor(String[] indices, String[] queries, ZkConfiguration zkConfOfTargetCluster) {
        super(indices, queries);
        _zkConfOfTargetCluster = zkConfOfTargetCluster;
    }

    @Override
    public void init(NodeContext nodeContext) throws Exception {
        _client = new MapFileClient(_zkConfOfTargetCluster);
    }

    @Override
    public void close(NodeContext nodeContext) throws Exception {
        _client.close();
        _client = null;
    }

    @Override
    public void execute(NodeContext nodeContext, String queryString) throws Exception {
        _client.get(queryString, indices);
    }

}
