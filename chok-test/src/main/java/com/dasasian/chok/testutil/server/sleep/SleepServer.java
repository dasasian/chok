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
package com.dasasian.chok.testutil.server.sleep;

import com.dasasian.chok.node.IContentServer;
import com.dasasian.chok.util.NodeConfiguration;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * This class implements the back-end side of a dummy server, to be used for
 * testing. It just sleeps for a while and then returns nothing.
 */
@ProtocolInfo(protocolName = "SleepServer", protocolVersion = 0L)
public class SleepServer implements IContentServer, ISleepServer {

    protected final Set<String> shards = Collections.synchronizedSet(new HashSet<>());
    protected String nodeName;
    private Random rand = new Random();

    @Override
    public long getProtocolVersion(final String protocol, final long clientVersion) {
        return 0L;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return null;
    }

    @Override
    public void init(String nodeName, NodeConfiguration nodeConfiguration) {
        this.nodeName = nodeName;
    }

    public void addShard(final String shardName, final File shardDir) {
        shards.add(shardName);
    }

    public void removeShard(final String shardName) {
        shards.remove(shardName);
    }

    @Override
    public Collection<String> getShards() {
        return shards;
    }

    public Map<String, String> getShardMetaData(final String shardName) {
        return new HashMap<>(0);
    }

    public void shutdown() {
        shards.clear();
    }

    public int sleep(long msec, int delta, String[] shards) throws IllegalArgumentException {
        if (shards != null) {
            String err = "";
            String sep = "";
            for (String shard : shards) {
                if (!this.shards.contains(shard)) {
                    System.err.println("Node " + nodeName + " does not have shard " + shard + "!!");
                    err += sep + shard;
                    sep = ", ";
                }
            }
            if (err.length() > 0) {
                throw new IllegalArgumentException("Node " + nodeName + " invalid shards: " + err);
            }
        }
        if (delta > 0) {
            msec = Math.max(0, msec + Math.round(((2.0 * rand.nextDouble()) - 1.0) * delta));
        }
        if (msec > 0) {
            try {
                Thread.sleep(msec);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return shards != null ? shards.length : 0;
    }

}
