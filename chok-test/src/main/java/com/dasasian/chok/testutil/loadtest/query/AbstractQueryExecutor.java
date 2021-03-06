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
package com.dasasian.chok.testutil.loadtest.query;

import com.dasasian.chok.node.NodeContext;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class AbstractQueryExecutor implements Serializable {

    protected final String[] indices;
    protected final String[] queries;

    public AbstractQueryExecutor(String[] indices, String[] queries) {
        this.indices = indices;
        this.queries = queries;
    }

    public String[] getQueries() {
        return queries;
    }

    public String[] getIndices() {
        return indices;
    }

    /**
     * Called from the loadtest node before calling
     * {@link #execute(NodeContext, String)} method.
     *
     * @param nodeContext the nodeContext to use
     * @throws Exception is an error occurs
     */
    public abstract void init(NodeContext nodeContext) throws Exception;

    /**
     * Called from the loadtest node after calling
     * {@link #execute(NodeContext, String)} method the last time.
     *
     * @param nodeContext the nodeContext to use
     * @throws Exception is an error occurs
     */
    public abstract void close(NodeContext nodeContext) throws Exception;

    /**
     * Might called multiple times from the loadtest node, depending on query
     * rate.
     *
     * @param nodeContext the nodeContext to use
     * @param query the query to execute
     * @throws Exception is an error occurs
     */
    public abstract void execute(NodeContext nodeContext, String query) throws Exception;

}
