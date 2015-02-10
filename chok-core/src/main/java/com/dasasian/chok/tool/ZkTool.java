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
package com.dasasian.chok.tool;

import com.dasasian.chok.util.ZkChokUtil;
import com.dasasian.chok.util.ZkConfiguration;
import com.dasasian.chok.util.ZkConfigurationLoader;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.cli.*;

import java.io.Serializable;
import java.util.List;

public class ZkTool {

    private ZkConfiguration conf;
    private ZkClient zkClient;

    public ZkTool() {
        conf = ZkConfigurationLoader.loadConfiguration();
        zkClient = ZkChokUtil.startZkClient(conf, 5000);
    }

    public static void main(String[] args) {
        final Options options = new Options();

        Option lsOption = new Option("ls", true, "list zp path contents");
        lsOption.setArgName("path");
        Option readOption = new Option("read", true, "read and print zp path contents");
        readOption.setArgName("path");
        Option rmOption = new Option("rm", true, "remove zk files");
        rmOption.setArgName("path");
        Option rmrOption = new Option("rmr", true, "remove zk directories");
        rmrOption.setArgName("path");

        OptionGroup actionGroup = new OptionGroup();
        actionGroup.setRequired(true);
        actionGroup.addOption(lsOption);
        actionGroup.addOption(readOption);
        actionGroup.addOption(rmOption);
        actionGroup.addOption(rmrOption);
        options.addOptionGroup(actionGroup);

        final CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            final CommandLine line = parser.parse(options, args);
            ZkTool zkTool = new ZkTool();
            if (line.hasOption(lsOption.getOpt())) {
                String path = line.getOptionValue(lsOption.getOpt());
                zkTool.ls(path);
            } else if (line.hasOption(readOption.getOpt())) {
                String path = line.getOptionValue(readOption.getOpt());
                zkTool.read(path);
            } else if (line.hasOption(rmOption.getOpt())) {
                String path = line.getOptionValue(rmOption.getOpt());
                zkTool.rm(path, false);
            } else if (line.hasOption(rmrOption.getOpt())) {
                String path = line.getOptionValue(rmrOption.getOpt());
                zkTool.rm(path, true);
            }
            zkTool.close();
        } catch (ParseException e) {
            System.out.println(e.getClass().getSimpleName() + ": " + e.getMessage());
            formatter.printHelp(ZkTool.class.getSimpleName(), options);
        }

    }

    public void ls(String path) {
        List<String> children = zkClient.getChildren(path);
        System.out.println(String.format("Found %s items", children.size()));
        if (path.charAt(path.length() - 1) != ZkConfiguration.PATH_SEPARATOR) {
            path += ZkConfiguration.PATH_SEPARATOR;
        }
        for (String child : children) {
            System.out.println(path + child);
        }
    }

    public void rm(String path, boolean recursiv) {
        if (recursiv) {
            zkClient.deleteRecursive(path);
        } else {
            zkClient.delete(path);
        }
    }

    public void read(String path) {
        Serializable data = zkClient.readData(path);
        System.out.println("from type: " + data.getClass().getName());
        System.out.println("to string: " + data.toString());
    }

    private void close() {
        zkClient.close();
    }

}
