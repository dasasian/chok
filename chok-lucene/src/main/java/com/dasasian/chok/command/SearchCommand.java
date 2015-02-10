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

import com.dasasian.chok.lucene.Hit;
import com.dasasian.chok.lucene.Hits;
import com.dasasian.chok.lucene.ILuceneClient;
import com.dasasian.chok.lucene.LuceneClient;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.ZkConfiguration;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class SearchCommand extends ProtocolCommand {

    private String[] _indexNames;
    private String _query;
    private int _count;
    public SearchCommand() {
        super("search", "<index name>[,<index name>,...] \"<query>\" [count]", "Search in supplied indices. The query should be in \". If you supply a result count hit details will be printed. To search in all indices write \"*\". This uses the client type LuceneClient.");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        CommandLineHelper.validateMinArguments(args, 3);
        _indexNames = args[1].split(",");
        _query = args[2];
        if (args.length > 3) {
            _count = Integer.parseInt(args[3]);
        }
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        if (_count > 0) {
            search(_indexNames, _query, _count);
        } else {
            search(_indexNames, _query);
        }
    }

    void search(final String[] indexNames, final String queryString, final int count) throws Exception {
        final ILuceneClient client = new LuceneClient();
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse(queryString);
        final long start = System.currentTimeMillis();
        final Hits hits = client.search(query, indexNames, count);
        final long end = System.currentTimeMillis();
        System.out.println(hits.size() + " hits found in " + ((end - start) / 1000.0) + "sec.");
        int index = 0;
        final CommandLineHelper.Table table = new CommandLineHelper.Table("Hit", "Node", "Shard", "DocId", "Score");
        for (final Hit hit : hits.getHits()) {
            table.addRow(index, hit.getNode(), hit.getShard(), hit.getDocId(), hit.getScore());
            index++;
        }
        System.out.println(table.toString());
        client.close();
    }

    void search(final String[] indexNames, final String queryString) throws Exception {
        final ILuceneClient client = new LuceneClient();
        final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse(queryString);
        final long start = System.currentTimeMillis();
        final int hitsSize = client.count(query, indexNames);
        final long end = System.currentTimeMillis();
        System.out.println(hitsSize + " Hits found in " + ((end - start) / 1000.0) + "sec.");
        client.close();
    }

}
