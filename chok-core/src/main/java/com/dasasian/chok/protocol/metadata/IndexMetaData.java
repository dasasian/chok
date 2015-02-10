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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexMetaData implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final String path;
    private int replicationLevel;
    private final Set<Shard> shards = Sets.newHashSet();
    private IndexDeployError deployError;

    public IndexMetaData(String name, String path, int replicationLevel) {
        this.name = name;
        this.path = path;
        this.replicationLevel = replicationLevel;
    }

    public String getPath() {
        return path;
    }

    public void setReplicationLevel(int replicationLevel) {
        this.replicationLevel = replicationLevel;
    }

    public int getReplicationLevel() {
        return replicationLevel;
    }

    public String getName() {
        return name;
    }

    public Set<Shard> getShards() {
        return shards;
    }

    public Shard getShard(String shardName) {
        for (Shard shard : shards) {
            if (shard.getName().equals(shardName)) {
                return shard;
            }
        }
        return null;
    }

    public String getShardPath(String shardName) {
        String shardPath = null;
        Shard shard = getShard(shardName);
        if (shard != null) {
            shardPath = shard.getPath();
        }
        return shardPath;
    }

    public void setDeployError(IndexDeployError deployError) {
        this.deployError = deployError;
    }

    public IndexDeployError getDeployError() {
        return deployError;
    }

    public boolean hasDeployError() {
        return deployError != null;
    }

    @Override
    public String toString() {
        return "name: " + name + ", replication: " + replicationLevel + ", path: " + path;
    }

    public static class Shard implements Serializable {

        private static final long serialVersionUID = IndexMetaData.serialVersionUID;
        private final String name;
        private final String path;
        private final Map<String, String> metaDataMap = Maps.newHashMap();

        public Shard(String name, String path) {
            this.name = name;
            this.path = path;
        }

        public String getName() {
            return name;
        }

        public String getPath() {
            return path;
        }

        public Map<String, String> getMetaDataMap() {
            return metaDataMap;
        }

        @Override
        public String toString() {
            return getName();
        }

        public static List<String> getShardNames(Collection<Shard> shards) {
            return ImmutableList.copyOf(Iterables.transform(shards, new Function<Shard, String>() {
                @Override
                public String apply(Shard shard) {
                    return shard.getName();
                }
            }));
        }

    }

}
