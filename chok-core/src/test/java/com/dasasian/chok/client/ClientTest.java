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

import com.dasasian.chok.protocol.IAddRemoveListener;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.protocol.metadata.IndexMetaData.Shard;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.server.simpletest.ISimpleTestServer;
import com.dasasian.chok.util.ClientConfiguration;
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.IZkDataListener;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ClientTest extends AbstractTest {

    @Test
    public void testAddRemoveIndexForSearching() throws Exception {
        InteractionProtocol protocol = mock(InteractionProtocol.class);
        Client client = new Client(ISimpleTestServer.class, new DefaultNodeSelectionPolicy(), protocol, new ClientConfiguration());
        IndexMetaData indexMD = new IndexMetaData("index1", "path", 1);
        indexMD.getShards().add(new Shard("shard1", "path"));
        indexMD.getShards().add(new Shard("shard2", "path"));
        client.addIndexForSearching(indexMD);
        verify(protocol, times(2)).registerChildListener(eq(client), eq(PathDef.SHARD_TO_NODES), anyString(), any(IAddRemoveListener.class));

        client.removeIndex(indexMD.getName());
        verify(protocol, times(2)).unregisterChildListener(eq(client), eq(PathDef.SHARD_TO_NODES), anyString());
    }

    @Test
    public void testAddRemoveIndexForWatching() throws Exception {
        InteractionProtocol protocol = mock(InteractionProtocol.class);
        Client client = new Client(ISimpleTestServer.class, new DefaultNodeSelectionPolicy(), protocol, new ClientConfiguration());
        IndexMetaData indexMD = new IndexMetaData("index1", "path", 1);
        indexMD.getShards().add(new Shard("shard1", "path"));
        indexMD.getShards().add(new Shard("shard2", "path"));
        client.addIndexForWatching(indexMD.getName());
        verify(protocol, times(1)).registerDataListener(eq(client), eq(PathDef.INDICES_METADATA), anyString(), any(IZkDataListener.class));

        client.removeIndex(indexMD.getName());
        verify(protocol, times(1)).unregisterDataChanges(eq(client), eq(PathDef.INDICES_METADATA), anyString());
    }

    @Test
    public void testClose() throws Exception {
        InteractionProtocol protocol = mock(InteractionProtocol.class);
        Client client = new Client(ISimpleTestServer.class, new DefaultNodeSelectionPolicy(), protocol, new ClientConfiguration());
        client.close();

        verify(protocol).unregisterComponent(client);
        verify(protocol).disconnect();
    }

}
