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
package com.dasasian.chok.protocol;

import com.dasasian.chok.operation.node.NodeOperation;
import com.dasasian.chok.operation.node.OperationResult;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NodeQueue extends BlockingQueue<NodeOperation> {

    private String _resultsPath;

    public NodeQueue(ZkClient zkClient, String rootPath) {
        super(zkClient, rootPath);
        _resultsPath = rootPath + "/results";
        this.zkClient.createPersistent(_resultsPath, true);

        // cleanup odd result situations
        List<String> results = this.zkClient.getChildren(_resultsPath);
        for (String elementName : results) {
            try {
                this.zkClient.delete(getElementPath(elementName));
            } catch (ZkNoNodeException e) {
                // ignore, can be already deleted by other queue instance
            }
        }
    }

    private String getResultPath(String elementId) {
        return _resultsPath + "/" + elementId;
    }

    public String add(NodeOperation element) {
        String elementName = super.add(element);
        zkClient.delete(getResultPath(elementName));
        return elementName;
    }

    public NodeOperation complete(OperationResult result) throws InterruptedException {
        Element<NodeOperation> element = getFirstElement();
        if (result != null) {
            zkClient.createEphemeral(getResultPath(element.getName()), result);
        }
        zkClient.delete(getElementPath(element.getName()));
        return element.getData();
    }

    public Serializable getResult(String elementId, boolean remove) {
        String zkPath = getResultPath(elementId);
        Serializable result = zkClient.readData(zkPath, true);
        if (remove) {
            zkClient.delete(zkPath);
        }
        return result;
    }

    public List<OperationResult> getResults() {
        List<String> childs = zkClient.getChildren(_resultsPath);
        List<OperationResult> watchdogs = new ArrayList<>(childs.size());
        for (String child : childs) {
            watchdogs.add(zkClient.readData(getResultPath(child)));
        }
        return watchdogs;
    }

}
