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
package com.dasasian.chok.protocol.metadata;

import com.dasasian.chok.util.One2ManyListMap;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import java.io.Serializable;
import java.util.List;

public class IndexDeployError implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String indexName;
    private final ErrorType errorType;
    private final One2ManyListMap<String, Exception> shard2ExceptionsMap = new One2ManyListMap<>();
    private String errorMessage;
    private String errorTrace;
    public IndexDeployError(String indexName, ErrorType errorType) {
        this.indexName = indexName;
        this.errorType = errorType;
    }
//    private Exception _exception;

    public String getIndexName() {
        return indexName;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getErrorTrace() {
        return errorTrace;
    }

    public void setException(Exception exception) {
        errorMessage = exception.getMessage();
        errorTrace = Throwables.getStackTraceAsString(exception);
    }

    public void addShardError(String shardName, Exception exception) {
        shard2ExceptionsMap.add(shardName, exception);
    }

//    public Exception getException() {
//        return _exception;
//    }

    public List<Exception> getShardErrors(String shardName) {
        return shard2ExceptionsMap.getValues(shardName);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(indexName).addValue(errorType).addValue(errorMessage).toString();
    }

    public static enum ErrorType {
        NO_NODES_AVAILIBLE, INDEX_NOT_ACCESSIBLE, SHARDS_NOT_DEPLOYABLE, UNKNOWN
    }
}
