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

import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.testutil.AbstractZkTest;
import com.dasasian.chok.testutil.TestIndex;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

public class DeployClientZkTest extends AbstractZkTest {

    public TestIndex testIndex = TestIndex.createTestIndex(temporaryFolder, 4);

    @Test
    public void testAddIndex() throws Exception {
        DeployClient deployClient = new DeployClient(protocol);
        IndexDeployFuture indexDeployFuture = (IndexDeployFuture) deployClient.addIndex(testIndex.getIndexName(), testIndex.getIndexUri(), 1, false);
        protocol.unregisterComponent(indexDeployFuture);
    }

    @Test
    public void testIndexAccess() throws Exception {
        IndexMetaData indexMD = new IndexMetaData("index1", new URI("indexPath"), 1, false);
        IDeployClient deployClient = new DeployClient(protocol);

        assertFalse(deployClient.existsIndex(indexMD.getName()));
        assertNull(deployClient.getIndexMetaData(indexMD.getName()));
        assertEquals(0, deployClient.getIndices().size());

        protocol.publishIndex(indexMD);
        assertTrue(deployClient.existsIndex(indexMD.getName()));
        assertNotNull(deployClient.getIndexMetaData(indexMD.getName()));
        assertEquals(1, deployClient.getIndices().size());
    }

    @Test
    public void testIndexRemove() throws Exception {
        IndexMetaData indexMD = new IndexMetaData("index1", new URI("indexPath"), 1, false);
        IDeployClient deployClient = new DeployClient(protocol);

        try {
            deployClient.removeIndex(indexMD.getName());
            fail("should throw exception");
        } catch (Exception e) {
            // expected
        }

        protocol.publishIndex(indexMD);
        deployClient.removeIndex(indexMD.getName());
        assertTrue(deployClient.existsIndex(indexMD.getName()));// undeploy
        // operation is send
        // to master
    }

}
