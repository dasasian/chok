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

import java.util.Set;

/**
 * Created by damith.chandrasekara on 8/25/15.
 */
public interface IResultReceiverWrapper<T> extends AutoCloseable {

    boolean isClosed();

    boolean isComplete();

    double getShardCoverage();

    void addNodeResult(T result, Set<String> shards);

    void addNodeError(Throwable error, Set<String> shards);

    Set<String> getAllShards();

    Set<String> getMissingShards();

    Set<String> getSeenShards();
}