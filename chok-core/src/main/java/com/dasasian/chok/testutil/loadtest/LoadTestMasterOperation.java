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
package com.dasasian.chok.testutil.loadtest;

import com.dasasian.chok.master.MasterContext;
import com.dasasian.chok.operation.OperationId;
import com.dasasian.chok.operation.master.MasterOperation;
import com.dasasian.chok.operation.node.OperationResult;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.testutil.loadtest.query.AbstractQueryExecutor;
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class LoadTestMasterOperation implements MasterOperation {

    private static final Logger LOG = Logger.getLogger(LoadTestMasterOperation.class);

    private String masterName;
    private final int numberOfTesterNodes;
    private final int startRate;
    private final int endRate;
    private final int step;

    private final long runTime;
    private final AbstractQueryExecutor queryExecutor;
    private final File resultDir;
    private final long startTime;
    private int currentIteration;
    private long currentIterationStartTime;

    public LoadTestMasterOperation(int nodes, int startRate, int endRate, int step, long runTime, AbstractQueryExecutor queryExecutor, File resultDir) {
        numberOfTesterNodes = nodes;
        this.startRate = startRate;
        this.endRate = endRate;
        this.step = step;
        this.queryExecutor = queryExecutor;
        this.runTime = runTime;
        this.resultDir = resultDir;

        startTime = System.currentTimeMillis();
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
        return ExecutionInstruction.EXECUTE;
    }

    private String getName() {
        return "load-test-" + startTime;
    }

    public void registerCompletion(InteractionProtocol protocol) {
        protocol.setFlag(getName());
    }

    public void joinCompletion(InteractionProtocol protocol) throws InterruptedException {
        while (protocol.flagExists(getName())) {
            Thread.sleep(1000);
        }
    }

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
        currentIterationStartTime = System.currentTimeMillis();
        if (masterName == null) {
            masterName = context.getMaster().getMasterName();
            resultDir.mkdirs();
            if (!resultDir.isDirectory()) {
                throw new IllegalStateException("result dir '" + resultDir.getAbsolutePath() + "' cannot be created");
            }
        }
        else if (!masterName.equals(context.getMaster().getMasterName())) {
            throw new IllegalStateException("master change detected - load test not safe for this since it writes local log files!");
        }
        List<String> testNodes = context.getProtocol().getLiveNodes();
        if (testNodes.size() < numberOfTesterNodes) {
            throw new IllegalStateException("only " + testNodes.size() + " available, needing " + numberOfTesterNodes);
        }
        testNodes = testNodes.subList(0, numberOfTesterNodes);

        final int queryRate = calculateCurrentQueryRate();
        if (currentIteration == 0) {
            LOG.info("starting load test with " + testNodes.size() + " nodes");
        }
        LOG.info("executing tests in iteration " + currentIteration + " at query rate: " + queryRate + " queries per second and with a run time of " + runTime / 1000 + " seconds");

        int remainingQueryRate = queryRate;
        int remainingNodes = testNodes.size();

        List<OperationId> nodeOperationIds = new ArrayList<>();
        for (String testNode : testNodes) {
            int queryRateForNode = remainingQueryRate / remainingNodes;
            LOG.info("instructing test on node " + testNode + " using query rate: " + queryRateForNode + " queries per second.");

            LoadTestNodeOperation nodeOperation = new LoadTestNodeOperation(queryExecutor, queryRateForNode, runTime);
            --remainingNodes;
            remainingQueryRate -= queryRateForNode;
            OperationId operationId = context.getProtocol().addNodeOperation(testNode, nodeOperation);
            nodeOperationIds.add(operationId);
        }
        return nodeOperationIds;
    }

    private int calculateCurrentQueryRate() {
        return startRate + currentIteration * step;
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> nodeResults) throws Exception {
        try {
            final int queryRate = calculateCurrentQueryRate();
            LOG.info("collecting results for iteration " + currentIteration + " and query rate " + queryRate + " after " + (System.currentTimeMillis() - currentIterationStartTime) + " ms ...");
            List<LoadTestQueryResult> queryResults = new ArrayList<>();
            for (OperationResult operationResult : nodeResults) {
                if (operationResult == null || operationResult.getUnhandledError() != null) {
                    Exception rootException = null;
                    if (operationResult != null) {
                        //rootException = operationResult.getUnhandledError();
                    }
                    throw new IllegalStateException("at least one node operation did not completed properly: " + nodeResults, rootException);
                }
                LoadTestNodeOperationResult nodeOperationResult = (LoadTestNodeOperationResult) operationResult;
                queryResults.addAll(nodeOperationResult.getQueryResults());
            }
            LOG.info("Received " + queryResults.size() + " queries, expected " + queryRate * runTime / 1000);

            File statisticsFile = new File(resultDir, "load-test-log-" + startTime + ".log");
            File resultsFile = new File(resultDir, "load-test-results-" + startTime + ".log");
            Writer statisticsWriter = new OutputStreamWriter(new FileOutputStream(statisticsFile, true));
            Writer resultWriter = new OutputStreamWriter(new FileOutputStream(resultsFile, true));
            if (currentIteration == 0) {
                // print headers
                statisticsWriter.append("#queryRate \tnode \tstartTime \tendTime \telapseTime \tquery \n");
                resultWriter.append("#requestedQueryRate \tachievedQueryRate \tfiredQueries \tqueryErrors \tavarageQueryDuration \tstandardDeviation  \n");
            }
            try {
                StorelessUnivariateStatistic timeStandardDeviation = new StandardDeviation();
                StorelessUnivariateStatistic timeMean = new Mean();
                int errors = 0;

                for (LoadTestQueryResult result : queryResults) {
                    long elapsedTime = result.getEndTime() > 0 ? result.getEndTime() - result.getStartTime() : -1;
                    statisticsWriter.write(queryRate + "\t" + result.getNodeId() + "\t" + result.getStartTime() + "\t" + result.getEndTime() + "\t" + elapsedTime + "\t" + result.getQuery() + "\n");
                    if (elapsedTime != -1) {
                        timeStandardDeviation.increment(elapsedTime);
                        timeMean.increment(elapsedTime);
                    }
                    else {
                        ++errors;
                    }
                }
                resultWriter.write(queryRate + "\t" + ((double) queryResults.size() / (runTime / 1000)) + "\t" + queryResults.size() + "\t" + errors + "\t" + (int) timeMean.getResult() + "\t" + (int) timeStandardDeviation.getResult() + "\n");
            }
            catch (IOException e) {
                throw new IllegalStateException("Failed to write statistics data.", e);
            }
            try {
                LOG.info("results written to " + resultsFile.getAbsolutePath());
                LOG.info("statistics written to " + statisticsFile.getAbsolutePath());
                statisticsWriter.close();
                resultWriter.close();
            }
            catch (IOException e) {
                LOG.warn("Failed to close statistics file.");
            }
            if (queryRate + step <= endRate) {
                currentIteration++;
                LOG.info("triggering next iteration " + currentIteration);
                context.getMasterQueue().add(this);
            }
            else {
                LOG.info("finish load test in iteration " + currentIteration + " after " + (System.currentTimeMillis() - startTime) + " ms");
                context.getProtocol().removeFlag(getName());
            }
        }
        catch (Exception e) {
            context.getProtocol().removeFlag(getName());
        }
    }
}
