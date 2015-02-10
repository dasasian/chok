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
package com.dasasian.chok.integration;

import com.dasasian.chok.client.Client;
import com.dasasian.chok.client.INodeProxyManager;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.TestIndex;
import com.dasasian.chok.testutil.TestNodeConfigurationFactory;
import com.dasasian.chok.testutil.integration.ChokMiniCluster;
import com.dasasian.chok.testutil.mockito.ChainedAnswer;
import com.dasasian.chok.testutil.mockito.PauseAnswer;
import com.dasasian.chok.testutil.server.simpletest.ISimpleTestServer;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class ClientIntegrationTest extends AbstractTest {

    @ClassRule
    public static ChokMiniCluster miniCluster = new ChokMiniCluster(SimpleTestServer.class, 2, 20000,TestNodeConfigurationFactory.class);

    public TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 2);

    @Test(timeout = 20000)
    public void testAddIndex_WithSlowProxyEstablishment() throws Exception {
        Client client = new Client(ISimpleTestServer.class, miniCluster.getZkConfiguration());
        INodeProxyManager proxyCreator = client.getProxyManager();
        INodeProxyManager proxyCreatorSpy = spy(proxyCreator);
        PauseAnswer<Void> pauseAnswer = new PauseAnswer<>(null);
        doAnswer(new ChainedAnswer(pauseAnswer, new CallsRealMethods())).when(proxyCreatorSpy).getProxy(anyString(), eq(true));
        client.setProxyCreator(proxyCreatorSpy);

        IndexMetaData indexMD = miniCluster.deployIndex(testIndex.getIndexName(), testIndex.getIndexFile(), miniCluster.getRunningNodeCount());
        pauseAnswer.joinExecutionBegin();
        assertThat(client.getSelectionPolicy().getShardNodes(indexMD.getShards().iterator().next().getName())).isEmpty();
        assertThat(client.getIndices()).isEmpty();
        pauseAnswer.resumeExecution(true);
        while (client.getSelectionPolicy().getShardNodes(indexMD.getShards().iterator().next().getName()).isEmpty()) {
            Thread.sleep(200);
        }
        assertThat(client.getSelectionPolicy().getShardNodes(indexMD.getShards().iterator().next().getName())).isNotEmpty();
        assertThat(client.getIndices()).isNotEmpty();

        client.setProxyCreator(proxyCreator);

        client.close();
    }

}
