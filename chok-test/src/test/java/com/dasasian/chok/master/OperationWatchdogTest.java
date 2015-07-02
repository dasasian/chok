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

import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.master.MasterOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.testutil.AbstractTest;
import com.dasasian.chok.testutil.Mocks;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.UtilModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OperationWatchdogTest extends AbstractTest {

    private MasterOperation masterOperation = Mockito.mock(MasterOperation.class);
    private List<OperationId> operationIds = Arrays.asList(new OperationId("node1", "ne1"));
    private OperationWatchdog watchdog = new OperationWatchdog("e1", masterOperation, operationIds);
    private InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
    private MasterQueue masterQueue = Mockito.mock(MasterQueue.class);
    private MasterContext context = new MasterContext(protocol, Mocks.mockMaster(), new DefaultDistributionPolicy(), masterQueue, injector.getInstance(ChokFileSystem.Factory.class));

    protected static Injector injector = Guice.createInjector(new UtilModule());

    @Test
    public void testWatchdogCompletion_OperationsDone() throws Exception {
        // start watchdog - node operation pending
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList(operationIds.get(0).getNodeName()));
        Mockito.when(protocol.isNodeOperationQueued(operationIds.get(0))).thenReturn(true);
        watchdog.start(context);
        assertFalse(watchdog.isDone());

        // finish node operation
        Mockito.when(protocol.isNodeOperationQueued(operationIds.get(0))).thenReturn(false);
        watchdog.checkDeploymentForCompletion();
        assertTrue(watchdog.isDone());
        Mockito.verify(masterQueue).removeWatchdog(watchdog);
    }

    @Test
    public void testWatchdogCompletion_NodeDown() throws Exception {
        // start watchdog - node operation pending
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList(operationIds.get(0).getNodeName()));
        Mockito.when(protocol.isNodeOperationQueued(operationIds.get(0))).thenReturn(true);
        watchdog.start(context);
        assertFalse(watchdog.isDone());

        // node down
        Mockito.when(protocol.getLiveNodes()).thenReturn(Collections.EMPTY_LIST);
        watchdog.checkDeploymentForCompletion();
        assertTrue(watchdog.isDone());
        Mockito.verify(masterQueue).removeWatchdog(watchdog);
    }
}
