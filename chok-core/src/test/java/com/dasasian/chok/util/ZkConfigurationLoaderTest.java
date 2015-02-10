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
package com.dasasian.chok.util;

import com.dasasian.chok.testutil.AbstractTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ZkConfigurationLoaderTest extends AbstractTest {

    @Test
    public void testLoader() {
        System.clearProperty(ChokConfiguration.CHOK_CONFIGURATION_HOME);
        ZkConfiguration conf = ZkConfigurationLoader.loadConfiguration();
        assertEquals("./zookeeper-data", conf.getDataDir());
    }

    @Test
    public void testSystemProperty() {
        try {
            System.setProperty(ChokConfiguration.CHOK_CONFIGURATION_HOME, "/alt");
            ZkConfiguration conf = ZkConfigurationLoader.loadConfiguration();
            assertEquals("./alt-zookeeper-data", conf.getDataDir());
        }
        finally {
            System.clearProperty(ChokConfiguration.CHOK_CONFIGURATION_HOME);
        }
    }

    @Test
    public void testZkParent() {
        System.clearProperty(ChokConfiguration.CHOK_CONFIGURATION_HOME);
        assertEquals("/chok", ZkConfiguration.getParent("/chok/abc"));
        assertEquals("/", ZkConfiguration.getParent("/chok"));
        assertEquals(null, ZkConfiguration.getParent("/"));
    }

}
