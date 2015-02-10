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

import com.dasasian.chok.util.ZkConfiguration;
import com.dasasian.chok.util.ZkConfigurationLoader;

import java.util.Map;

/**
 * User: damith.chandrasekara
 * Date: 7/6/13
 */
public abstract class Command {

    private final String command;
    private final String parameterString;
    private final String description;

    public Command(String command, String parameterString, String description) {
        this.command = command;
        this.parameterString = parameterString;
        this.description = description;
    }

    public final void parseArguments(String[] args) throws Exception {
        parseArguments(ZkConfigurationLoader.loadConfiguration(), args, CommandLineHelper.parseOptionMap(args));
    }

    public final void parseArguments(ZkConfiguration zkConf, String[] args) throws Exception {
        parseArguments(zkConf, args, CommandLineHelper.parseOptionMap(args));
    }

    protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) throws Exception {
        // subclasses may override
    }

    public void execute() throws Exception {
        execute(ZkConfigurationLoader.loadConfiguration());
    }

    protected abstract void execute(ZkConfiguration zkConf) throws Exception;

    public String getCommand() {
        return command;
    }

    public String getParameterString() {
        return parameterString;
    }

    public String getDescription() {
        return description;
    }
}
