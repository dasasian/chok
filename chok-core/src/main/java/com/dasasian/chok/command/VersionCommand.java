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
package com.dasasian.chok.command;

import com.dasasian.chok.protocol.metadata.Version;
import com.dasasian.chok.util.ZkConfiguration;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class VersionCommand extends Command {

    public VersionCommand() {
        super("version", "", "Print the version");
    }

    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
        Version versionInfo = Version.readFromJar();
        System.out.println("Chok '" + versionInfo.getNumber() + "'");
        System.out.println("Git-Revision '" + versionInfo.getRevision() + "'");
        System.out.println("Compiled by '" + versionInfo.getCreatedBy() + "' on '" + versionInfo.getCompileTime() + "'");
    }
}
