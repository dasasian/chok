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
package com.dasasian.chok.protocol.upgrade;

import com.dasasian.chok.operation.master.IndexReinitializeOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.util.List;

class UpgradeAction05_06 implements UpgradeAction {

    private static final Logger LOG = Logger.getLogger(UpgradeAction05_06.class);

    @SuppressWarnings("deprecation")
    @Override
    public void upgrade(InteractionProtocol protocol) {
        ZkClient zkClient = protocol.getZkClient();
        ZkConfiguration zkConf = protocol.getZkConfiguration();
        ZkClient zkClientForWriables = new ZkClient(zkConf.getServers(), 5000, 5000, new WriteableZkSerializer(com.dasasian.chok.index.IndexMetaData.class));
        LOG.info("restoring indices meta data...");
        String oldIndicesPath = getOldIndicesPath(zkConf);
        List<String> indices = zkClient.getChildren(oldIndicesPath);
        LOG.info("found " + indices.size() + " old indices");
        for (String indexName : indices) {
            com.dasasian.chok.index.IndexMetaData oldIndexMD = zkClientForWriables.readData(oldIndicesPath + "/" + indexName);
            IndexMetaData newIndexMD = new IndexMetaData(indexName, oldIndexMD.getPath(), oldIndexMD.getReplicationLevel());
            IndexReinitializeOperation deployOperation = new IndexReinitializeOperation(newIndexMD);
            protocol.addMasterOperation(deployOperation);
            protocol.publishIndex(newIndexMD);
        }
        zkClientForWriables.close();

        LOG.info("deleting obsolete folders...");
        zkClient.deleteRecursive(zkConf.getRootPath() + "/" + "indexes");
        zkClient.deleteRecursive(zkConf.getRootPath() + "/" + "node-to-shard");
        zkClient.deleteRecursive(zkConf.getRootPath() + "/" + "shard-to-node");
        zkClient.deleteRecursive(zkConf.getRootPath() + "/" + "loadtest-nodes");
        zkClient.deleteRecursive(zkConf.getRootPath() + "/" + "server-metrics");
        zkClient.deleteRecursive(zkConf.getRootPath() + "/" + "shard-to-error");

        LOG.info("upgrade done");
    }

    protected static String getOldIndicesPath(ZkConfiguration zkConf) {
        return zkConf.getRootPath() + "/" + "indexes";
    }

    static class WriteableZkSerializer implements ZkSerializer {

        private final Class<? extends Writable> _writableClass;

        public WriteableZkSerializer(Class<? extends Writable> writableClass) {
            _writableClass = writableClass;
        }

        @Override
        public Object deserialize(byte[] bytes) throws ZkMarshallingError {
            DataInputBuffer buffer = new DataInputBuffer();
            buffer.reset(bytes, bytes.length);
            try {
                Writable instance = _writableClass.newInstance();
                instance.readFields(buffer);
                buffer.close();
                return instance;
            }
            catch (Exception e) {
                throw new ZkMarshallingError(e);
            }
        }

        @Override
        public byte[] serialize(Object data) throws ZkMarshallingError {
            DataOutputBuffer out = new DataOutputBuffer();
            try {
                ((Writable) data).write(out);
                return out.getData();
            }
            catch (Exception e) {
                throw new ZkMarshallingError(e);
            }
            finally {
                IOUtils.closeStream(out);
            }
        }

    }

}
