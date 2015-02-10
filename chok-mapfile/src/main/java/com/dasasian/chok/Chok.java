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
package com.dasasian.chok;

import com.dasasian.chok.client.DeployClient;
import com.dasasian.chok.client.IDeployClient;
import com.dasasian.chok.client.IIndexDeployFuture;
import com.dasasian.chok.client.IndexState;
import com.dasasian.chok.command.*;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.StringUtil;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Provides command line access to a Chok cluster.
 */
public class Chok {

    protected static final Logger LOG = Logger.getLogger(CommandLineHelper.class);
    private final static List<com.dasasian.chok.command.Command> COMMANDS = new ArrayList<>();
    protected static com.dasasian.chok.command.Command START_NODE_COMMAND = new StartMapFileNodeCommand();
    static {
        COMMANDS.add(new StartZkCommand());
        COMMANDS.add(new StartMasterCommand());
        COMMANDS.add(START_NODE_COMMAND);
        COMMANDS.add(new ListIndicesCommand());
        COMMANDS.add(new ListNodesCommand());
        COMMANDS.add(new ListErrorsCommand());
        COMMANDS.add(new AddIndexCommand());
        COMMANDS.add(new RemoveIndexCommand());
        COMMANDS.add(new RedeployIndexCommand());
        COMMANDS.add(new CheckCommand());
        COMMANDS.add(new VersionCommand());
        COMMANDS.add(new ShowStructureCommand());
        COMMANDS.add(new StartGuiCommand());
        COMMANDS.add(new LogMetricsCommand());
        COMMANDS.add(new RunClassCommand());

        Set<String> commandStrings = new HashSet<>();
        for (com.dasasian.chok.command.Command command : COMMANDS) {
            if (!commandStrings.add(command.getCommand())) {
                throw new IllegalStateException("duplicated command sting " + command.getCommand());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsageAndExit();
        }
        boolean showStackTrace = parseOptionMap(args).containsKey("-s");
        if (showStackTrace) {
            args = removeArg(args, "-s");
        }
        com.dasasian.chok.command.Command command = null;
        try {
            command = getCommand(args[0]);
            command.parseArguments(args);
            command.execute();
        } catch (Exception e) {
            printError(e.getMessage());
            if (showStackTrace) {
                e.printStackTrace();
            }
            if (command != null) {
                printUsageHeader();
                printUsage(command);
                printUsageFooter();
            }
            // e.printStackTrace();
            System.exit(1);
        }
    }

    private static String[] removeArg(String[] args, String argToRemove) {
        List<String> newArgs = new ArrayList<>();
        for (String arg : args) {
            if (!arg.equals(argToRemove)) {
                newArgs.add(arg);
            }
        }
        return newArgs.toArray(new String[newArgs.size()]);
    }

    protected static com.dasasian.chok.command.Command getCommand(String commandString) {
        for (com.dasasian.chok.command.Command command : COMMANDS) {
            if (commandString.equalsIgnoreCase(command.getCommand())) {
                return command;
            }
        }
        throw new IllegalArgumentException("no command for '" + commandString + "' found");
    }

    private static void printUsage(com.dasasian.chok.command.Command command) {
        System.err.println("  " + StringUtil.fillWithWhiteSpace(command.getCommand() + " " + command.getParameterString(), 60) + " " + command.getDescription());
    }

    private static void printUsageAndExit() {
        printUsageHeader();
        for (com.dasasian.chok.command.Command command : COMMANDS) {
            printUsage(command);
        }
        printUsageFooter();
        System.exit(1);
    }

    private static void printUsageFooter() {
        System.err.println();
        System.err.println("Global Options:");
        System.err.println("  -s\t\tshow stacktrace");
        System.err.println();
    }

    private static void printUsageHeader() {
        System.err.println("Usage: ");
    }

    protected static Map<String, String> parseOptionMap(final String[] args) {
        Map<String, String> optionMap = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                String value = null;
                if (i < args.length - 1 && !args[i + 1].startsWith("-")) {
                    value = args[i + 1];
                }
                optionMap.put(args[i], value);
            }
        }
        return optionMap;
    }

    protected static void addIndex(InteractionProtocol protocol, String name, String path, int replicationLevel) {
        IDeployClient deployClient = new DeployClient(protocol);
        if (name.trim().equals("*")) {
            throw new IllegalArgumentException("Index with name " + name + " isn't allowed.");
        }
        if (deployClient.existsIndex(name)) {
            throw new IllegalArgumentException("Index with name " + name + " already exists.");
        }

        try {
            long startTime = System.currentTimeMillis();
            IIndexDeployFuture deployFuture = deployClient.addIndex(name, path, replicationLevel);
            while (true) {
                long duration = System.currentTimeMillis() - startTime;
                if (deployFuture.getState() == IndexState.DEPLOYED) {
                    System.out.println("\ndeployed index '" + name + "' in " + duration + " ms");
                    break;
                } else if (deployFuture.getState() == IndexState.ERROR) {
                    System.err.println("\nfailed to deploy index '" + name + "' in " + duration + " ms");
                    break;
                }
                System.out.print(".");
                deployFuture.joinDeployment(1000);
            }
        } catch (final InterruptedException e) {
            printError("interrupted wait on index deployment");
        }
    }

    protected static void removeIndex(InteractionProtocol protocol, final String indexName) {
        IDeployClient deployClient = new DeployClient(protocol);
        if (!deployClient.existsIndex(indexName)) {
            throw new IllegalArgumentException("index '" + indexName + "' does not exist");
        }
        deployClient.removeIndex(indexName);
    }

    protected static void validateMinArguments(String[] args, int minCount) {
        if (args.length < minCount) {
            throw new IllegalArgumentException("not enough arguments");
        }
    }

    private static void printError(String errorMsg) {
        System.err.println("ERROR: " + errorMsg);
    }


}
