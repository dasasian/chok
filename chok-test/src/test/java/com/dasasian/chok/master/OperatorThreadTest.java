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
import com.dasasian.chok.operation.master.MasterOperation.ExecutionInstruction;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.testutil.Mocks;
import com.dasasian.chok.testutil.TestUtil;
import com.dasasian.chok.testutil.mockito.SleepingAnswer;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class OperatorThreadTest {

    protected static final List EMPTY_LIST = Collections.EMPTY_LIST;

    private final InteractionProtocol protocol = Mockito.mock(InteractionProtocol.class);
    private final MasterQueue queue = Mockito.mock(MasterQueue.class);
    protected final MasterContext context = new MasterContext(protocol, Mocks.mockMaster(), new DefaultDistributionPolicy(), queue);

    @Test(timeout = 10000)
    public void testSafeMode() throws Exception {
        final MasterOperation operation = mockOperation(ExecutionInstruction.EXECUTE);
        Mockito.when(queue.peek()).thenReturn(operation).thenAnswer(new SleepingAnswer());
        Mockito.when(protocol.getLiveNodes()).thenReturn(EMPTY_LIST);

        long safeModeMaxTime = 200;
        OperatorThread operatorThread = new OperatorThread(context, safeModeMaxTime);
        operatorThread.start();

        // no nodes connected
        Thread.sleep(safeModeMaxTime + 200);
        assertTrue(operatorThread.isAlive());
        assertTrue(operatorThread.isInSafeMode());

        // connect nodes
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));

        // check safe mode & operation execution
        Thread.sleep(safeModeMaxTime + 200);
        assertTrue(operatorThread.isAlive());
        assertFalse(operatorThread.isInSafeMode());
        Mockito.verify(operation, Mockito.times(1)).execute(context, EMPTY_LIST);
        operatorThread.interrupt();
    }

    @Test(timeout = 10000)
    public void testGracefulShutdownWhileInSafeMode() throws Exception {
        Mockito.when(queue.peek()).thenAnswer(new SleepingAnswer());

        long safeModeMaxTime = 2000;
        OperatorThread operatorThread = new OperatorThread(context, safeModeMaxTime);
        operatorThread.start();

        assertTrue(operatorThread.isAlive());
        assertTrue(operatorThread.isInSafeMode());
        operatorThread.interrupt();
        operatorThread.join();
    }

    @Test(timeout = 10000)
    public void testGracefulShutdownWhileWaitingForOperations() throws Exception {
        Mockito.when(queue.peek()).thenAnswer(new SleepingAnswer());
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
        long safeModeMaxTime = 200;
        OperatorThread operatorThread = new OperatorThread(context, safeModeMaxTime);
        operatorThread.start();

        waitUntilLeaveSafeMode(operatorThread);
        assertTrue(operatorThread.isAlive());
        assertFalse(operatorThread.isInSafeMode());
        operatorThread.interrupt();
        operatorThread.join();
    }

    @Test(timeout = 10000)
    public void testOperationExecution() throws Exception {
        final MasterOperation masterOperation1 = mockOperation(ExecutionInstruction.EXECUTE);
        final MasterOperation masterOperation2 = mockOperation(ExecutionInstruction.EXECUTE);
        final MasterOperation masterOperation3 = mockOperation(ExecutionInstruction.EXECUTE);
        Mockito.when(queue.peek()).thenReturn(masterOperation1).thenReturn(masterOperation2).thenReturn(masterOperation3).thenAnswer(new SleepingAnswer());

        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
        long safeModeMaxTime = 200;
        OperatorThread operatorThread = new OperatorThread(context, safeModeMaxTime);
        operatorThread.start();
        waitUntilLeaveSafeMode(operatorThread);
        // Thread.sleep(safeModeMaxTime + 100);

        InOrder inOrder = Mockito.inOrder(masterOperation1, masterOperation2, masterOperation3);
        inOrder.verify(masterOperation1, Mockito.times(1)).execute(context, EMPTY_LIST);
        inOrder.verify(masterOperation2, Mockito.times(1)).execute(context, EMPTY_LIST);
        inOrder.verify(masterOperation3, Mockito.times(1)).execute(context, EMPTY_LIST);
        operatorThread.interrupt();
        operatorThread.join();
    }

    @Test(timeout = 10000)
    public void testOperationWatchdog() throws Exception {
        String nodeName = "node1";
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList(nodeName));
        List<OperationId> operationIds = new ArrayList<>();
        operationIds.add(new OperationId(nodeName, "e1"));

        final MasterOperation leaderOperation = Mockito.mock(MasterOperation.class);
        Mockito.when(leaderOperation.execute(context, EMPTY_LIST)).thenReturn(operationIds);
        setupExecutionInstruction(leaderOperation, ExecutionInstruction.EXECUTE);
        Mockito.when(queue.peek()).thenReturn(leaderOperation).thenAnswer(new SleepingAnswer());

        Mockito.when(protocol.isNodeOperationQueued(operationIds.get(0))).thenReturn(false);
        OperationWatchdog watchdog = Mockito.mock(OperationWatchdog.class);
        Mockito.when(queue.moveOperationToWatching(leaderOperation, operationIds)).thenReturn(watchdog);

        // start the operator
        long safeModeMaxTime = 200;
        final OperatorThread operatorThread = new OperatorThread(context, safeModeMaxTime);
        operatorThread.start();
        waitUntilLeaveSafeMode(operatorThread);

        Mockito.verify(watchdog).start(context);
        operatorThread.interrupt();
        operatorThread.join();
    }

    private void waitUntilLeaveSafeMode(final OperatorThread operatorThread) throws Exception {
        TestUtil.waitUntil(false, operatorThread::isInSafeMode, TimeUnit.SECONDS, 30);
    }

    @Test(timeout = 10000)
    public void testOperationLocks_CancelLockedOperation() throws Exception {
        final MasterOperation leaderOperation1 = Mockito.mock(MasterOperation.class);
        final MasterOperation leaderOperation2 = Mockito.mock(MasterOperation.class);
        ExecutionInstruction lockInstruction = MasterOperation.ExecutionInstruction.CANCEL;

        runLockSituation(leaderOperation1, leaderOperation2, lockInstruction);
        Mockito.verify(leaderOperation2, Mockito.times(0)).execute(context, EMPTY_LIST);
    }

    @Test(timeout = 10000)
    public void testOperationLocks_SuspendLockedTask() throws Exception {
        final MasterOperation leaderOperation1 = Mockito.mock(MasterOperation.class);
        final MasterOperation leaderOperation2 = Mockito.mock(MasterOperation.class);

        ExecutionInstruction lockInstruction = MasterOperation.ExecutionInstruction.ADD_TO_QUEUE_TAIL;
        runLockSituation(leaderOperation1, leaderOperation2, lockInstruction);
        Mockito.verify(leaderOperation2, Mockito.times(0)).execute(context, EMPTY_LIST);
        Mockito.verify(queue, Mockito.times(1)).add(leaderOperation2);
    }

    @Test(timeout = 10000)
    public void testRecreateWatchdogs() throws Exception {
        Mockito.when(queue.peek()).thenAnswer(new SleepingAnswer());
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
        OperationWatchdog watchdog1 = Mockito.mock(OperationWatchdog.class);
        OperationWatchdog watchdog2 = Mockito.mock(OperationWatchdog.class);
        Mockito.when(watchdog1.isDone()).thenReturn(true);
        Mockito.when(watchdog2.isDone()).thenReturn(false);
        Mockito.when(queue.getWatchdogs()).thenReturn(Arrays.asList(watchdog1, watchdog2));

        OperatorThread operatorThread = new OperatorThread(context, 100);
        operatorThread.start();
        waitUntilLeaveSafeMode(operatorThread);
        Thread.sleep(200);

        Mockito.verify(queue).getWatchdogs();
        Mockito.verify(watchdog1).isDone();
        Mockito.verify(watchdog2).isDone();
        Mockito.verify(queue, Mockito.times(1)).removeWatchdog(watchdog1);
        Mockito.verify(queue, Mockito.times(0)).removeWatchdog(watchdog2);
        assertTrue(operatorThread.getOperationRegistry().getRunningOperations().contains(watchdog2.getOperation()));
    }

    @Test(timeout = 10000)
    public void testInterruptedException_Queue() throws Exception {
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
        Mockito.when(queue.peek()).thenThrow(new InterruptedException());
        OperatorThread operatorThread = new OperatorThread(context, 50);
        operatorThread.start();
        operatorThread.join();
    }

    @Test(timeout = 10000)
    public void testInterruptedException_Queue_Zk() throws Exception {
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
        ZkInterruptedException zkInterruptedException = Mockito.mock(ZkInterruptedException.class);
        Mockito.when(queue.peek()).thenThrow(zkInterruptedException);
        OperatorThread operatorThread = new OperatorThread(context, 50);
        operatorThread.start();
        operatorThread.join();
    }

    @Test(timeout = 10000)
    public void testInterruptedException_Operation() throws Exception {
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
        final MasterOperation masterOperation = mockOperation(ExecutionInstruction.EXECUTE);
        Mockito.when(masterOperation.execute(context, EMPTY_LIST)).thenThrow(new InterruptedException());
        Mockito.when(queue.peek()).thenReturn(masterOperation);
        OperatorThread operatorThread = new OperatorThread(context, 50);
        operatorThread.start();
        operatorThread.join();
    }

    @Test(timeout = 1000000)
    public void testInterruptedException_Operation_Zk() throws Exception {
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
        ZkInterruptedException zkInterruptedException = Mockito.mock(ZkInterruptedException.class);
        final MasterOperation masterOperation = mockOperation(ExecutionInstruction.EXECUTE);
        Mockito.when(masterOperation.execute(context, EMPTY_LIST)).thenThrow(zkInterruptedException);
        Mockito.when(queue.peek()).thenReturn(masterOperation);
        OperatorThread operatorThread = new OperatorThread(context, 50);
        operatorThread.start();
        operatorThread.join();
    }

    @Test(timeout = 10000000)
    public void testDontStopOnOOM() throws Exception {
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
        Mockito.when(queue.peek()).thenThrow(new OutOfMemoryError("test exception")).thenAnswer(new SleepingAnswer());

        OperatorThread operatorThread = new OperatorThread(context, 50);
        operatorThread.start();
        operatorThread.join(500);

        assertEquals(true, operatorThread.isAlive());
        operatorThread.interrupt();
        Mockito.verify(queue, Mockito.atLeast(2)).peek();
    }

    private void runLockSituation(final MasterOperation leaderOperation1, final MasterOperation leaderOperation2, ExecutionInstruction instruction) throws Exception {
        String nodeName = "node1";
        Mockito.when(protocol.getLiveNodes()).thenReturn(Arrays.asList(nodeName));
        List<OperationId> operationIds = new ArrayList<>();
        operationIds.add(new OperationId(nodeName, "e1"));

        Mockito.when(protocol.isNodeOperationQueued(operationIds.get(0))).thenReturn(false);
        OperationWatchdog watchdog = Mockito.mock(OperationWatchdog.class);
        Mockito.when(queue.moveOperationToWatching(leaderOperation1, operationIds)).thenReturn(watchdog);

        setupExecutionInstruction(leaderOperation1, ExecutionInstruction.EXECUTE);
        setupExecutionInstruction(leaderOperation2, instruction);
        Mockito.when(leaderOperation1.execute(context, EMPTY_LIST)).thenReturn(operationIds);
        Mockito.when(queue.peek()).thenReturn(leaderOperation1).thenReturn(leaderOperation2).thenAnswer(new SleepingAnswer());

        // start the operator
        long safeModeMaxTime = 200;
        OperatorThread operatorThread = new OperatorThread(context, safeModeMaxTime);
        operatorThread.start();

        // let operation1 be executed
        waitUntilLeaveSafeMode(operatorThread);
        // Thread.sleep(safeModeMaxTime + 100);
        Mockito.verify(leaderOperation1, Mockito.times(1)).execute(context, EMPTY_LIST);

        operatorThread.interrupt();
        operatorThread.join();
    }

    private MasterOperation mockOperation(ExecutionInstruction instruction) throws Exception {
        MasterOperation masterOperation = Mockito.mock(MasterOperation.class);
        Mockito.when(masterOperation.getExecutionInstruction((List<MasterOperation>) Matchers.notNull())).thenReturn(instruction);
        return masterOperation;
    }

    private void setupExecutionInstruction(final MasterOperation leaderOperation, ExecutionInstruction instruction) throws Exception {
        Mockito.when(leaderOperation.getExecutionInstruction((List<MasterOperation>) Matchers.notNull())).thenReturn(instruction);
    }

}
