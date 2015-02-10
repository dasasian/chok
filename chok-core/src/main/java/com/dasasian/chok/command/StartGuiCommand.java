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

import com.dasasian.chok.util.WebApp;
import com.dasasian.chok.util.ZkConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class StartGuiCommand extends Command {

    private int _port = 8080;
    private File _war;
    public StartGuiCommand() {
        super("startGui", "[-war <pathToWar>] [-port <port>]", "Starts the web based chok.gui");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
        if (optionMap.containsKey("-war")) {
            _war = new File(optionMap.get("-war"));
        }
        if (optionMap.containsKey("-port")) {
            _port = Integer.parseInt(optionMap.get("-port"));
        }
    }

    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
        List<String> paths = new ArrayList<>();
        if (_war != null) {
            paths.add(_war.getAbsolutePath());
        } else {
            paths.add(".");
            paths.add("./extras/chok.gui");
        }

        WebApp app = new WebApp(paths.toArray(new String[paths.size()]), _port);
        app.startWebServer();
    }
}
