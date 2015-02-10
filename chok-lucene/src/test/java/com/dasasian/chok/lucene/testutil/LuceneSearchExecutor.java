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
package com.dasasian.chok.lucene.testutil;

import com.dasasian.chok.lucene.ILuceneClient;
import com.dasasian.chok.lucene.LuceneClient;
import com.dasasian.chok.node.NodeContext;
import com.dasasian.chok.testutil.loadtest.query.AbstractQueryExecutor;
import com.dasasian.chok.util.ZkConfiguration;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

@SuppressWarnings("serial")
public class LuceneSearchExecutor extends AbstractQueryExecutor {

    private final int count;
    private ILuceneClient client;
    private final ZkConfiguration zkConfOfTargetCluster;

    public LuceneSearchExecutor(String[] indices, String[] queries, ZkConfiguration zkConfOfTargetCluster, int count) {
        super(indices, queries);
        this.zkConfOfTargetCluster = zkConfOfTargetCluster;
        this.count = count;
    }

    @Override
    public void init(NodeContext nodeContext) throws Exception {
        client = new LuceneClient(zkConfOfTargetCluster);
    }

    @Override
    public void close(NodeContext nodeContext) throws Exception {
        client.close();
        client = null;
    }

    @Override
    public void execute(NodeContext nodeContext, String queryString) throws Exception {
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse(queryString);
        client.search(query, indices, count);
    }

}
