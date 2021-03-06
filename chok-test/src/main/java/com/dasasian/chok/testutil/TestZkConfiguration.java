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

import com.dasasian.chok.util.ZkConfiguration;
import com.dasasian.chok.util.ZkConfigurationLoader;

import java.io.File;

/**
 * User: damith.chandrasekara
 * Date: 7/5/13
 */
public class TestZkConfiguration {
    public static ZkConfiguration getTestConfiguration(File temporaryDir) {
        return getTestConfiguration(temporaryDir, 2181, "/chok");
    }

    public static ZkConfiguration getTestConfiguration(File temporaryDir, int port, String zkRootPath) {
        File dataDir = new File(temporaryDir, "data");
        File logDir = new File(temporaryDir, "log");
        return ZkConfigurationLoader.createConfiguration(port, zkRootPath, dataDir, logDir);
    }
}
