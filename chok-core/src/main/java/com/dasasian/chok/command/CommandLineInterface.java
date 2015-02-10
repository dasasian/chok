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

import com.dasasian.chok.util.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Provides command line access to a Chok cluster.
 */
public class CommandLineInterface {

    private static final Logger LOG = Logger.getLogger(CommandLineInterface.class);

    private static final Set<String> COMMAND_STRINGS = Sets.newHashSet();
    private final static List<Command> COMMANDS = Lists.newArrayList();

    static {
        addCommand(new StartZkCommand());
        addCommand(new StartMasterCommand());
        addCommand(new ListIndicesCommand());
        addCommand(new ListNodesCommand());
        addCommand(new ListErrorsCommand());
        addCommand(new AddIndexCommand());
        addCommand(new RemoveIndexCommand());
        addCommand(new RedeployIndexCommand());
        addCommand(new CheckCommand());
        addCommand(new VersionCommand());
        addCommand(new ShowStructureCommand());
        addCommand(new StartGuiCommand());
        addCommand(new LogMetricsCommand());
        addCommand(new RunClassCommand());
    }

    protected static void addCommand(Command command) {
        if (!COMMAND_STRINGS.add(command.getCommand())) {
            throw new IllegalStateException("duplicated command sting " + command.getCommand());
        }
        COMMANDS.add(command);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsageAndExit();
        }
        boolean showStackTrace = parseOptionMap(args).containsKey("-s");
        if (showStackTrace) {
            args = removeArg(args, "-s");
        }
        Command command = null;
        try {
            command = getCommand(args[0]);
            command.parseArguments(args);
            command.execute();
        }
        catch (Exception e) {
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

    protected static Command getCommand(String commandString) {
        for (Command command : COMMANDS) {
            if (commandString.equalsIgnoreCase(command.getCommand())) {
                return command;
            }
        }
        throw new IllegalArgumentException("no command for '" + commandString + "' found");
    }

    private static void printUsage(Command command) {
        System.err.println("  " + StringUtil.fillWithWhiteSpace(command.getCommand() + " " + command.getParameterString(), 60) + " " + command.getDescription());
    }

    private static void printUsageAndExit() {
        printUsageHeader();
        for (Command command : COMMANDS) {
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

    private static void printError(String errorMsg) {
        System.err.println("ERROR: " + errorMsg);
    }

}
