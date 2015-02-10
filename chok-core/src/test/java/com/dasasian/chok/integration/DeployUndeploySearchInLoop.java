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

import com.dasasian.chok.client.DeployClient;
import com.dasasian.chok.testutil.TestZkConfiguration;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestClient;
import com.dasasian.chok.util.ZkChokUtil;
import com.dasasian.chok.util.ZkConfiguration;
import org.apache.log4j.Logger;
import org.junit.Ignore;

/**
 * - start a chok-cluster<br>
 * - deploy one or more indices<br>
 * - run this class<br>
 */
@Ignore
public class DeployUndeploySearchInLoop {

    private static Logger LOG = Logger.getLogger(DeployUndeploySearchInLoop.class);

    public static void main(String[] args) throws Exception {
        SimpleTestClient testClient = new SimpleTestClient();
        // todo fix at some point
        ZkConfiguration zkConfig = TestZkConfiguration.getTestConfiguration(null);
        DeployClient deployClient = new DeployClient(ZkChokUtil.startZkClient(zkConfig, 60000), zkConfig);

        int runThroughs = 2;
        while (true) {
            try {
                String indexName = "index" + runThroughs;
                LOG.info("deploying index '" + indexName + "'");
                deployClient.addIndex(indexName, "/Users/jz/Documents/workspace/ms/katta/src/test/testIndexA", 1).joinDeployment();
            }
            catch (Exception e) {
                logException("deploy", e);
            }

            try {
                String indexName = "index" + (runThroughs - 1);
                LOG.info("undeploying index '" + indexName + "'");
                deployClient.removeIndex(indexName);
            }
            catch (Exception e) {
                logException("undeploy", e);
            }

            try {
                String result = testClient.testRequest("query", new String[]{"*"});
                LOG.info(runThroughs + ": got result: " + result);
            }
            catch (Exception e) {
                logException("search", e);
            }
            Thread.sleep(5000);
            runThroughs++;
            LOG.info(testClient.getClient().getSelectionPolicy());
        }
    }

    private static void logException(String category, Exception e) {
        LOG.error("got " + category + " exception", e);
        // System.out.println("------------THREAD DUMP:");
        // System.out.println(StringUtil.getThreadDump());
        // System.out.println("-------------------------------------");
    }
}
