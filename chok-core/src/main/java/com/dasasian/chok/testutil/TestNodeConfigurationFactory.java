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
package com.dasasian.chok.testutil;

import com.dasasian.chok.util.NodeConfiguration;
import com.dasasian.chok.util.NodeConfigurationLoader;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * User: damith.chandrasekara
 * Date: 7/4/13
 */
public class TestNodeConfigurationFactory implements NodeConfigurationFactory {

    private final TemporaryFolder temporaryFolder;

    public TestNodeConfigurationFactory(TemporaryFolder temporaryFolder) {
        this.temporaryFolder = temporaryFolder;
    }

    public NodeConfiguration getConfiguration() {
        return getConfiguration(20000);
    }

    public NodeConfiguration getConfiguration(int nodeStartPort) {
        try {
            return NodeConfigurationLoader.createConfiguration(nodeStartPort, temporaryFolder.newFolder());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
