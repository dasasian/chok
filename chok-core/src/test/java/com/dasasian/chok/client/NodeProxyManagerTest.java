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
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.net.ConnectException;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class NodeProxyManagerTest extends AbstractTest {

    private INodeSelectionPolicy _nodeSelectionPolicy = mock(INodeSelectionPolicy.class);
    private NodeProxyManager _proxyManager = new NodeProxyManager(ISimpleTestServer.class, new Configuration(), _nodeSelectionPolicy);

    @Test
    public void testProxyFailure() throws Exception {
        NodeProxyManager proxyManagerSpy = spy(_proxyManager);
        IContentServer contentServer = mock(IContentServer.class);
        doReturn(contentServer).when(proxyManagerSpy).createNodeProxy(anyString());

        assertThat(proxyManagerSpy.getProxy("node1", true)).isNotNull();
        assertThat(proxyManagerSpy.getProxy("node2", true)).isNotNull();
        proxyManagerSpy.setSuccessiveProxyFailuresBeforeReestablishing(2);

        // node1 failure
        reportNodeFailure(proxyManagerSpy, "node1");
        verifyNoMoreInteractions(_nodeSelectionPolicy);
        assertThat(proxyManagerSpy.getProxy("node1", false)).isNotNull();
        assertThat(proxyManagerSpy.getProxy("node2", false)).isNotNull();

        // node2 failure
        reportNodeFailure(proxyManagerSpy, "node2");
        verifyNoMoreInteractions(_nodeSelectionPolicy);
        assertThat(proxyManagerSpy.getProxy("node1", false)).isNotNull();
        assertThat(proxyManagerSpy.getProxy("node2", false)).isNotNull();

        // node1 success
        proxyManagerSpy.reportNodeCommunicationSuccess("node1");

        // node1 failure
        reportNodeFailure(proxyManagerSpy, "node1");
        verifyNoMoreInteractions(_nodeSelectionPolicy);
        assertThat(proxyManagerSpy.getProxy("node1", false)).isNotNull();
        assertThat(proxyManagerSpy.getProxy("node2", false)).isNotNull();

        // node2 failure
        reportNodeFailure(proxyManagerSpy, "node2");
        verify(_nodeSelectionPolicy).removeNode("node2");
        verifyNoMoreInteractions(_nodeSelectionPolicy);
        assertThat(proxyManagerSpy.getProxy("node1", false)).isNotNull();
        assertThat(proxyManagerSpy.getProxy("node2", false)).isNull();
    }

    @Test
    public void testProxyFailure_ConnectionFailure() throws Exception {
        NodeProxyManager proxyManagerSpy = spy(_proxyManager);
        IContentServer contentServer = mock(IContentServer.class);
        doReturn(contentServer).when(proxyManagerSpy).createNodeProxy(anyString());

        assertThat(proxyManagerSpy.getProxy("node1", true)).isNotNull();
        assertThat(proxyManagerSpy.getProxy("node2", true)).isNotNull();
        proxyManagerSpy.setSuccessiveProxyFailuresBeforeReestablishing(2);

        // node1 connect failure
        reportNodeFailure(proxyManagerSpy, "node1", new InvocationTargetException(new ConnectException()));
        verify(_nodeSelectionPolicy).removeNode("node1");
        verifyNoMoreInteractions(_nodeSelectionPolicy);
        assertThat(proxyManagerSpy.getProxy("node1", false)).isNull();
        assertThat(proxyManagerSpy.getProxy("node2", false)).isNotNull();
    }

    private void reportNodeFailure(NodeProxyManager proxyManagerSpy, String nodeName) {
        reportNodeFailure(proxyManagerSpy, nodeName, new RuntimeException());
    }

    private void reportNodeFailure(NodeProxyManager proxyManagerSpy, String nodeName, Exception exception) {
        try {
            proxyManagerSpy.reportNodeCommunicationFailure(nodeName, exception);
        }
        catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("not a proxy instance");
        }
    }
}
