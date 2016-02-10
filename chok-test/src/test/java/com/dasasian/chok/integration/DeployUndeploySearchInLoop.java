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
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.testutil.TestZkConfiguration;
import com.dasasian.chok.testutil.server.simpletest.SimpleTestClient;
import com.dasasian.chok.util.ZkChokUtil;
import com.dasasian.chok.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.junit.Ignore;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

/**
 * - start a chok-cluster<br>
 * - deploy one or more indices<br>
 * - run this class<br>
 */
@Ignore
public class DeployUndeploySearchInLoop {

    private static Logger LOG = LoggerFactory.getLogger(DeployUndeploySearchInLoop.class);

    // todo need to run this somehow
    public static void main(String[] args) throws Exception {
        final SimpleTestClient testClient = new SimpleTestClient();
        // todo fix at some point
        final ZkConfiguration zkConfig = TestZkConfiguration.getTestConfiguration(null);
        final ZkClient zkClient = ZkChokUtil.startZkClient(zkConfig, 60000);
        final InteractionProtocol interactionProtocol = new InteractionProtocol(zkClient, zkConfig);
        DeployClient deployClient = new DeployClient(interactionProtocol);

        int runThroughs = 2;
        while (true) {
            try {
                String indexName = "index" + runThroughs;
                LOG.info("deploying index '" + indexName + "'");
                deployClient.addIndex(indexName, new URI("/Users/jz/Documents/workspace/ms/katta/src/test/testIndexA"), 1, false).joinDeployment();
            } catch (Exception e) {
                logException("deploy", e);
            }

            try {
                String indexName = "index" + (runThroughs - 1);
                LOG.info("undeploying index '" + indexName + "'");
                deployClient.removeIndex(indexName);
            } catch (Exception e) {
                logException("undeploy", e);
            }

            try {
                String[] result = testClient.testRequest("query", new String[]{"*"});
                LOG.info(runThroughs + ": got result: " + result);
            } catch (Exception e) {
                logException("search", e);
            }
            Thread.sleep(5000);
            runThroughs++;
            LOG.info(testClient.getClient().getSelectionPolicy().toString());
        }
    }

    private static void logException(String category, Exception e) {
        LOG.error("got " + category + " exception", e);
        // System.out.println("------------THREAD DUMP:");
        // System.out.println(StringUtil.getThreadDump());
        // System.out.println("-------------------------------------");
    }
}
