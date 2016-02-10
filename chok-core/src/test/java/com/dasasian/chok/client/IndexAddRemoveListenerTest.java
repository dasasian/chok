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
import com.dasasian.chok.util.ZkConfiguration.PathDef;
import org.I0Itec.zkclient.IZkDataListener;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.net.URI;

/**
 * Tests for IndexAddRemoveListener
 *
 * Created by damith.chandrasekara on 8/27/15.
 */
public class IndexAddRemoveListenerTest {

    @Test
    public void testAddRemoveIndexForSearching() throws Exception {
        InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
        INodeProxyManager proxyManager = Mockito.mock(INodeProxyManager.class);
        IndexAddRemoveListener indexAddRemoveListener = new IndexAddRemoveListener(new DefaultNodeSelectionPolicy(), protocol, proxyManager);
        final URI uri = new URI("path");
        IndexMetaData indexMD = new IndexMetaData("index1", uri, 1, false);
        indexMD.getShards().add(new Shard("shard1", uri));
        indexMD.getShards().add(new Shard("shard2", uri));
        indexAddRemoveListener.addIndexForSearching(indexMD);
        Mockito.verify(protocol, Mockito.times(2)).registerChildListener(Matchers.eq(indexAddRemoveListener), Matchers.eq(PathDef.SHARD_TO_NODES), Matchers.anyString(), Matchers.any(IAddRemoveListener.class));

        indexAddRemoveListener.removeIndex(indexMD.getName());
        Mockito.verify(protocol, Mockito.times(2)).unregisterChildListener(Matchers.eq(indexAddRemoveListener), Matchers.eq(PathDef.SHARD_TO_NODES), Matchers.anyString());
    }

    @Test
    public void testAddRemoveIndexForWatching() throws Exception {
        InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
        INodeProxyManager proxyManager = Mockito.mock(INodeProxyManager.class);
        IndexAddRemoveListener indexAddRemoveListener = new IndexAddRemoveListener(new DefaultNodeSelectionPolicy(), protocol, proxyManager);
        final URI uri = new URI("path");
        IndexMetaData indexMD = new IndexMetaData("index1", uri, 1, false);
        indexMD.getShards().add(new Shard("shard1", uri));
        indexMD.getShards().add(new Shard("shard2", uri));
        indexAddRemoveListener.addIndexForWatching(indexMD.getName());
        Mockito.verify(protocol, Mockito.times(1)).registerDataListener(Matchers.eq(indexAddRemoveListener), Matchers.eq(PathDef.INDICES_METADATA), Matchers.anyString(), Matchers.any(IZkDataListener.class));

        indexAddRemoveListener.removeIndex(indexMD.getName());
        Mockito.verify(protocol, Mockito.times(1)).unregisterDataChanges(Matchers.eq(indexAddRemoveListener), Matchers.eq(PathDef.INDICES_METADATA), Matchers.anyString());
    }

    @Test
    public void testClose() throws Exception {
        InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
        INodeProxyManager proxyManager = Mockito.mock(INodeProxyManager.class);
        IndexAddRemoveListener indexAddRemoveListener = new IndexAddRemoveListener(new DefaultNodeSelectionPolicy(), protocol, proxyManager);

        indexAddRemoveListener.close();
        Mockito.verify(protocol).unregisterComponent(indexAddRemoveListener);
    }

}
