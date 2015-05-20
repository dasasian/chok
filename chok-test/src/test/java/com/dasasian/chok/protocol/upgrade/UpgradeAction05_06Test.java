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
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.protocol.upgrade.UpgradeAction05_06.WriteableZkSerializer;
import com.dasasian.chok.testutil.AbstractZkTest;
import com.dasasian.chok.testutil.Mocks;
import com.dasasian.chok.util.ZkChokUtil;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class UpgradeAction05_06Test extends AbstractZkTest {

    @Test
    @SuppressWarnings("deprecation")
    public void testPreserveIndices() throws Exception {
        ZkClient zkClientForWritables = ZkChokUtil.startZkClient(zk.getZkConfiguration().getServers(), 5000, 5000, new WriteableZkSerializer(com.dasasian.chok.index.IndexMetaData.class));
        String indexName = "index1";
        com.dasasian.chok.index.IndexMetaData oldIndexMD = new com.dasasian.chok.index.IndexMetaData("indexPath", "analyzer", 2, com.dasasian.chok.index.IndexMetaData.IndexState.DEPLOYED);
        String oldIndicesPath = UpgradeAction05_06.getOldIndicesPath(zk.getZkConfiguration());
        zkClientForWritables.createPersistent(oldIndicesPath);
        zkClientForWritables.createPersistent(oldIndicesPath + "/" + indexName, oldIndexMD);
        zkClientForWritables.close();

        UpgradeAction05_06 upgradeAction = new UpgradeAction05_06();
        upgradeAction.upgrade(protocol);

        assertEquals(1, protocol.getIndices().size());
        IndexMetaData newIndexMD = protocol.getIndexMD(indexName);
        assertEquals(indexName, newIndexMD.getName());
        assertEquals(oldIndexMD.getPath(), newIndexMD.getPath());
        assertEquals(oldIndexMD.getReplicationLevel(), newIndexMD.getReplicationLevel());

        MasterQueue queue = protocol.publishMaster(Mocks.mockMaster());
        assertEquals(1, queue.size());
        assertThat(queue.peek(), instanceOf(IndexReinitializeOperation.class));
    }
}
