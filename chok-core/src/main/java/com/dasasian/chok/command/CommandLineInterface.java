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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

/**
 * Provides command line access to a Chok cluster.
 */
public class CommandLineInterface {

    private static final Logger LOG = LoggerFactory.getLogger(CommandLineInterface.class);

    private final Set<String> commandStrings = Sets.newHashSet();
    private final List<Command> commands = Lists.newArrayList();

    @Inject protected StartZkCommand startZkCommand;
    @Inject protected StartMasterCommand startMasterCommand;
    @Inject protected ListIndicesCommand listIndicesCommand;
    @Inject protected ListNodesCommand listNodesCommand;
    @Inject protected ListErrorsCommand listErrorsCommand;
    @Inject protected AddIndexCommand addIndexCommand;
    @Inject protected RemoveIndexCommand removeIndexCommand;
    @Inject protected RedeployIndexCommand redeployIndexCommand;
    @Inject protected CheckCommand checkCommand;
    @Inject protected VersionCommand versionCommand;
    @Inject protected ShowStructureCommand showStructureCommand;
    @Inject protected LogMetricsCommand logMetricsCommand;
    @Inject protected RunClassCommand runClassCommand;
    @Inject protected IndexAutoRepairCommand indexAutoRepairCommand;
    @Inject protected StatusCommand statusCommand;
    @Inject protected HealthcheckCommand healthcheckCommand;
    @Inject protected RemoveNodeCommand removeNodeCommand;

    protected void addBaseCommands() {
        addCommand(startZkCommand);
        addCommand(startMasterCommand);
        addCommand(listIndicesCommand);
        addCommand(listNodesCommand);
        addCommand(listErrorsCommand);
        addCommand(addIndexCommand);
        addCommand(removeIndexCommand);
        addCommand(redeployIndexCommand);
        addCommand(checkCommand);
        addCommand(versionCommand);
        addCommand(showStructureCommand);
        addCommand(logMetricsCommand);
        addCommand(runClassCommand);
        addCommand(indexAutoRepairCommand);
        addCommand(statusCommand);
        addCommand(healthcheckCommand);
        addCommand(removeNodeCommand);
    }

    protected void addCommand(Command command) {
        if (!commandStrings.add(command.getCommand())) {
            throw new IllegalStateException("duplicated command sting " + command.getCommand());
        }
        commands.add(command);
    }

    protected void execute(String[] args) {
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

    private String[] removeArg(String[] args, String argToRemove) {
        List<String> newArgs = new ArrayList<>();
        for (String arg : args) {
            if (!arg.equals(argToRemove)) {
                newArgs.add(arg);
            }
        }
        return newArgs.toArray(new String[newArgs.size()]);
    }

    protected Command getCommand(String commandString) {
        for (Command command : commands) {
            if (commandString.equalsIgnoreCase(command.getCommand())) {
                return command;
            }
        }
        throw new IllegalArgumentException("no command for '" + commandString + "' found");
    }

    private void printUsage(Command command) {
        System.err.println("  " + StringUtil.fillWithWhiteSpace(command.getCommand() + " " + command.getParameterString(), 60) + " " + command.getDescription());
    }

    private void printUsageAndExit() {
        printUsageHeader();
        commands.forEach((command) -> printUsage(command));
        printUsageFooter();
        System.exit(1);
    }

    private void printUsageFooter() {
        System.err.println();
        System.err.println("Global Options:");
        System.err.println("  -s\t\tshow stacktrace");
        System.err.println();
    }

    private void printUsageHeader() {
        System.err.println("Usage: ");
    }

    protected Map<String, String> parseOptionMap(final String[] args) {
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

    private void printError(String errorMsg) {
        System.err.println("ERROR: " + errorMsg);
    }

}
