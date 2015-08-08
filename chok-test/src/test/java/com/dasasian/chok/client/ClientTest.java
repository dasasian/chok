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
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.net.URI;

public class ClientTest extends AbstractTest {

    @Test
    public void testAddRemoveIndexForSearching() throws Exception {
        InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
        Client client = new Client(ISimpleTestServer.class, new DefaultNodeSelectionPolicy(), protocol, new ClientConfiguration());
        final URI uri = new URI("path");
        IndexMetaData indexMD = new IndexMetaData("index1", uri, 1, false);
        indexMD.getShards().add(new Shard("shard1", uri));
        indexMD.getShards().add(new Shard("shard2", uri));
        client.addIndexForSearching(indexMD);
        Mockito.verify(protocol, Mockito.times(2)).registerChildListener(Matchers.eq(client), Matchers.eq(PathDef.SHARD_TO_NODES), Matchers.anyString(), Matchers.any(IAddRemoveListener.class));

        client.removeIndex(indexMD.getName());
        Mockito.verify(protocol, Mockito.times(2)).unregisterChildListener(Matchers.eq(client), Matchers.eq(PathDef.SHARD_TO_NODES), Matchers.anyString());
    }

    @Test
    public void testAddRemoveIndexForWatching() throws Exception {
        InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
        Client client = new Client(ISimpleTestServer.class, new DefaultNodeSelectionPolicy(), protocol, new ClientConfiguration());
        final URI uri = new URI("path");
        IndexMetaData indexMD = new IndexMetaData("index1", uri, 1, false);
        indexMD.getShards().add(new Shard("shard1", uri));
        indexMD.getShards().add(new Shard("shard2", uri));
        client.addIndexForWatching(indexMD.getName());
        Mockito.verify(protocol, Mockito.times(1)).registerDataListener(Matchers.eq(client), Matchers.eq(PathDef.INDICES_METADATA), Matchers.anyString(), Matchers.any(IZkDataListener.class));

        client.removeIndex(indexMD.getName());
        Mockito.verify(protocol, Mockito.times(1)).unregisterDataChanges(Matchers.eq(client), Matchers.eq(PathDef.INDICES_METADATA), Matchers.anyString());
    }

    @Test
    public void testClose() throws Exception {
        InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
        Client client = new Client(ISimpleTestServer.class, new DefaultNodeSelectionPolicy(), protocol, new ClientConfiguration());
        client.close();

        Mockito.verify(protocol).unregisterComponent(client);
        Mockito.verify(protocol).disconnect();
    }

}
