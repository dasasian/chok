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

import com.dasasian.chok.node.IContentServer;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.server.simpletest.ISimpleTestServer;
import org.apache.hadoop.conf.Configuration;
import org.fest.assertions.Assertions;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.net.ConnectException;

public class NodeProxyManagerTest extends AbstractTest {

    private INodeSelectionPolicy _nodeSelectionPolicy = Mockito.mock(INodeSelectionPolicy.class);
    private NodeProxyManager _proxyManager = new NodeProxyManager(ISimpleTestServer.class, new Configuration(), _nodeSelectionPolicy);

    @Test
    public void testProxyFailure() throws Exception {
        NodeProxyManager proxyManagerSpy = Mockito.spy(_proxyManager);
        IContentServer contentServer = Mockito.mock(IContentServer.class);
        Mockito.doReturn(contentServer).when(proxyManagerSpy).createNodeProxy(Matchers.anyString());

        Assertions.assertThat(proxyManagerSpy.getProxy("node1", true)).isNotNull();
        Assertions.assertThat(proxyManagerSpy.getProxy("node2", true)).isNotNull();
        proxyManagerSpy.setSuccessiveProxyFailuresBeforeReestablishing(2);

        // node1 failure
        reportNodeFailure(proxyManagerSpy, "node1");
        Mockito.verifyNoMoreInteractions(_nodeSelectionPolicy);
        Assertions.assertThat(proxyManagerSpy.getProxy("node1", false)).isNotNull();
        Assertions.assertThat(proxyManagerSpy.getProxy("node2", false)).isNotNull();

        // node2 failure
        reportNodeFailure(proxyManagerSpy, "node2");
        Mockito.verifyNoMoreInteractions(_nodeSelectionPolicy);
        Assertions.assertThat(proxyManagerSpy.getProxy("node1", false)).isNotNull();
        Assertions.assertThat(proxyManagerSpy.getProxy("node2", false)).isNotNull();

        // node1 success
        proxyManagerSpy.reportNodeCommunicationSuccess("node1");

        // node1 failure
        reportNodeFailure(proxyManagerSpy, "node1");
        Mockito.verifyNoMoreInteractions(_nodeSelectionPolicy);
        Assertions.assertThat(proxyManagerSpy.getProxy("node1", false)).isNotNull();
        Assertions.assertThat(proxyManagerSpy.getProxy("node2", false)).isNotNull();

        // node2 failure
        reportNodeFailure(proxyManagerSpy, "node2");
        Mockito.verify(_nodeSelectionPolicy).removeNode("node2");
        Mockito.verifyNoMoreInteractions(_nodeSelectionPolicy);
        Assertions.assertThat(proxyManagerSpy.getProxy("node1", false)).isNotNull();
        Assertions.assertThat(proxyManagerSpy.getProxy("node2", false)).isNull();
    }

    @Test
    public void testProxyFailure_ConnectionFailure() throws Exception {
        NodeProxyManager proxyManagerSpy = Mockito.spy(_proxyManager);
        IContentServer contentServer = Mockito.mock(IContentServer.class);
        Mockito.doReturn(contentServer).when(proxyManagerSpy).createNodeProxy(Matchers.anyString());

        Assertions.assertThat(proxyManagerSpy.getProxy("node1", true)).isNotNull();
        Assertions.assertThat(proxyManagerSpy.getProxy("node2", true)).isNotNull();
        proxyManagerSpy.setSuccessiveProxyFailuresBeforeReestablishing(2);

        // node1 connect failure
        reportNodeFailure(proxyManagerSpy, "node1", new InvocationTargetException(new ConnectException()));
        Mockito.verify(_nodeSelectionPolicy).removeNode("node1");
        Mockito.verifyNoMoreInteractions(_nodeSelectionPolicy);
        Assertions.assertThat(proxyManagerSpy.getProxy("node1", false)).isNull();
        Assertions.assertThat(proxyManagerSpy.getProxy("node2", false)).isNotNull();
    }

    private void reportNodeFailure(NodeProxyManager proxyManagerSpy, String nodeName) {
        reportNodeFailure(proxyManagerSpy, nodeName, new RuntimeException());
    }

    private void reportNodeFailure(NodeProxyManager proxyManagerSpy, String nodeName, Exception exception) {
        try {
            proxyManagerSpy.reportNodeCommunicationFailure(nodeName, exception);
        } catch (IllegalArgumentException e) {
            Assertions.assertThat(e).hasMessage("not a proxy instance");
        }
    }
}
