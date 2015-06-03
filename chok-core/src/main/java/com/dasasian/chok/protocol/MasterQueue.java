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

import com.dasasian.chok.master.OperationWatchdog;
import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.master.MasterOperation;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.util.ArrayList;
import java.util.List;

public class MasterQueue extends BlockingQueue<MasterOperation> {

    private String _watchdogsPath;

    public MasterQueue(ZkClient zkClient, String rootPath) {
        super(zkClient, rootPath);
        _watchdogsPath = rootPath + "/watchdogs";
        this.zkClient.createPersistent(_watchdogsPath, true);

        // cleanup odd watchdog situations
        List<String> watchdogs = this.zkClient.getChildren(_watchdogsPath);
        for (String elementName : watchdogs) {
            try {
                this.zkClient.delete(getElementPath(elementName));
            } catch (ZkNoNodeException e) {
                // ignore, can be already deleted by other queue instance
            }
        }
    }

    private String getWatchdogPath(String elementId) {
        return _watchdogsPath + "/" + elementId;
    }

    /**
     * Moves the top of the queue to the watching state.
     *
     * @param masterOperation  the master operation
     * @param nodeOperationIds  the node operation ids
     * @throws InterruptedException when interrupted
     * @return the operation watchdog class
     */
    public OperationWatchdog moveOperationToWatching(MasterOperation masterOperation, List<OperationId> nodeOperationIds) throws InterruptedException {
        Element<MasterOperation> element = getFirstElement();
        // we don't use the persisted operation cause the given masterOperation can
        // have a changed state
        OperationWatchdog watchdog = new OperationWatchdog(element.getName(), masterOperation, nodeOperationIds);
        zkClient.createPersistent(getWatchdogPath(element.getName()), watchdog);
        zkClient.delete(getElementPath(element.getName()));
        return watchdog;
    }

    public List<OperationWatchdog> getWatchdogs() {
        List<String> childs = zkClient.getChildren(_watchdogsPath);
        List<OperationWatchdog> watchdogs = new ArrayList<>(childs.size());
        for (String child : childs) {
            watchdogs.add(zkClient.readData(getWatchdogPath(child)));
        }
        return watchdogs;
    }

    public void removeWatchdog(OperationWatchdog watchdog) {
        zkClient.delete(getWatchdogPath(watchdog.getQueueElementId()));
    }

}
