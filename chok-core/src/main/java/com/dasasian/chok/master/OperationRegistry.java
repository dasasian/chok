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
package com.dasasian.chok.master;

import com.dasasian.chok.operation.master.MasterOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OperationRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(OperationRegistry.class);

    private final MasterContext context;
    private final List<OperationWatchdog> watchdogs = new ArrayList<>();

    public OperationRegistry(MasterContext context) {
        this.context = context;
    }

    public synchronized void watchFor(OperationWatchdog watchdog) {
        LOG.info("watch operation '" + watchdog.getOperation() + "' for node operations " + watchdog.getOperationIds());
        releaseDoneWatchdogs(); // lazy cleaning
        watchdogs.add(watchdog);
        watchdog.start(context);
    }

    private void releaseDoneWatchdogs() {
        for (Iterator<OperationWatchdog> iterator = watchdogs.iterator(); iterator.hasNext(); ) {
            OperationWatchdog watchdog = iterator.next();
            if (watchdog.isDone()) {
                context.getMasterQueue().removeWatchdog(watchdog);
                iterator.remove();
            }
        }
    }

    public synchronized List<MasterOperation> getRunningOperations() {
        List<MasterOperation> operations = new ArrayList<>();
        for (Iterator<OperationWatchdog> iterator = watchdogs.iterator(); iterator.hasNext(); ) {
            OperationWatchdog watchdog = iterator.next();
            if (watchdog.isDone()) {
                iterator.remove(); // lazy cleaning
            } else {
                operations.add(watchdog.getOperation());
            }
        }
        return operations;
    }

    public synchronized void shutdown() {
        for (Iterator<OperationWatchdog> iterator = watchdogs.iterator(); iterator.hasNext(); ) {
            OperationWatchdog watchdog = iterator.next();
            watchdog.cancel();
            iterator.remove();
        }
    }

}
