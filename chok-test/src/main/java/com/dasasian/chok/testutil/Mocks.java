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
package com.dasasian.chok.testutil;

import com.dasasian.chok.master.Master;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.protocol.NodeQueue;
import com.dasasian.chok.protocol.metadata.NodeMetaData;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class Mocks {

    private static int _nodeCounter;
    private static int _masterCounter;

    public static Master mockMaster() {
        Master master = Mockito.mock(Master.class);
        Mockito.when(master.getMasterName()).thenReturn("master" + _masterCounter++);
        return master;
    }

    public static MasterQueue publishMaster(InteractionProtocol protocol) {
        Master master = mockMaster();
        return protocol.publishMaster(master);
    }

    public static Node mockNode() {
        Node node = Mockito.mock(Node.class);
        Mockito.when(node.getName()).thenReturn("node" + _nodeCounter++);
        return node;
    }

    public static List<Node> mockNodes(int count) {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(mockNode());
        }
        return nodes;
    }

    public static NodeQueue publishNode(InteractionProtocol protocol, Node node) {
        return protocol.publishNode(node, new NodeMetaData(node.getName()));
    }

    public static List<NodeQueue> publishNodes(InteractionProtocol protocol, List<Node> nodes) {
        List<NodeQueue> nodeQueues = new ArrayList<>();
        for (Node node : nodes) {
            nodeQueues.add(publishNode(protocol, node));
        }
        return nodeQueues;
    }

}
