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
package com.dasasian.chok.mapfile;

import com.dasasian.chok.util.ChokException;

import java.util.List;

/**
 * The public interface to the front end of the MapFile server.
 */
public interface IMapFileClient {

    /**
     * Get all entries with the given key.
     *
     * @param key        The entry(s) to look up.
     * @param indexNames The MapFiles to search.
     * @return All the entries with the given key.
     * @throws com.dasasian.chok.util.ChokException if an error occurs
     */
    public List<String> get(String key, final String[] indexNames) throws ChokException;

    /**
     * Closes down the client.
     */
    public void close();

}