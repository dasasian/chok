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
package com.dasasian.chok.lucene;

import com.dasasian.chok.util.NodeConfiguration;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;

public class DefaultSearcherFactory implements ISearcherFactory {

    @Override
    public void init(NodeConfiguration config) {
        // nothing todo
    }

    @Override
    public IndexSearcher createSearcher(String shardName, Path shardDir) throws IOException {
        return new IndexSearcher(FSDirectory.open(shardDir.toAbsolutePath().toFile()));
    }

}
