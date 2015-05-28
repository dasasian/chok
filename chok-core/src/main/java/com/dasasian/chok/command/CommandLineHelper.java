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

import com.dasasian.chok.client.DeployClient;
import com.dasasian.chok.client.IDeployClient;
import com.dasasian.chok.client.IIndexDeployFuture;
import com.dasasian.chok.client.IndexState;
import com.dasasian.chok.protocol.InteractionProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides command line access to a Chok cluster.
 */
public class CommandLineHelper {

    protected static final Logger LOG = LoggerFactory.getLogger(CommandLineHelper.class);

    public static Map<String, String> parseOptionMap(final String[] args) {
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

    public static void addIndex(InteractionProtocol protocol, String name, String path, int replicationLevel) {
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

    public static void removeIndex(InteractionProtocol protocol, final String indexName) {
        IDeployClient deployClient = new DeployClient(protocol);
        if (!deployClient.existsIndex(indexName)) {
            throw new IllegalArgumentException("index '" + indexName + "' does not exist");
        }
        deployClient.removeIndex(indexName);
    }

    public static void validateMinArguments(String[] args, int minCount) {
        if (args.length < minCount) {
            throw new IllegalArgumentException("not enough arguments");
        }
    }

    private static void printError(String errorMsg) {
        System.err.println("ERROR: " + errorMsg);
    }

    public static class Table {

        private final List<String[]> rows = new ArrayList<>();
        private String[] header;
        private boolean batchMode;
        private boolean skipColumnNames;

        public Table(final String... header) {
            this.header = header;
        }

        /**
         * Set the header later by calling setHeader()
         */
        public Table() {
            // default constructor
        }

        public void setHeader(String... header) {
            this.header = header;
        }

        public boolean isBatchMode() {
            return batchMode;
        }

        public void setBatchMode(boolean batchMode) {
            this.batchMode = batchMode;
        }

        public boolean isSkipColumnNames() {
            return skipColumnNames;
        }

        public void setSkipColumnNames(boolean skipCoulmnNames) {
            this.skipColumnNames = skipCoulmnNames;
        }

        public void addRow(final Object... row) {
            String[] strs = new String[row.length];
            for (int i = 0; i < row.length; i++) {
                strs[i] = row[i] != null ? row[i].toString() : "";
            }
            rows.add(strs);
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            final int[] columnSizes = getColumnSizes();
            int rowWidth = 0;
            for (final int columnSize : columnSizes) {
                rowWidth += columnSize;
            }
            rowWidth += 2 + (Math.max(0, columnSizes.length - 1) * 3) + 2;
            String leftPad = "";
            if (!batchMode) {
                builder.append('\n').append(getChar(rowWidth, "-")).append('\n');
            }
            if (!skipColumnNames) {
                // Header.
                if (!batchMode) {
                    builder.append("| ");
                }
                for (int i = 0; i < header.length; i++) {
                    final String column = header[i];
                    builder.append(leftPad);
                    builder.append(column).append(getChar(columnSizes[i] - column.length(), " "));
                    if (!batchMode) {
                        leftPad = " | ";
                    } else {
                        leftPad = " ";
                    }
                }
                if (!batchMode) {
                    builder.append(" |\n").append(getChar(rowWidth, "="));// .append('\n');
                }
                builder.append('\n');
            }
            // Rows.
            for (final Object[] row : rows) {
                if (!batchMode) {
                    builder.append("| ");
                }
                leftPad = "";
                for (int i = 0; i < row.length; i++) {
                    builder.append(leftPad);
                    builder.append(row[i]);
                    builder.append(getChar(columnSizes[i] - row[i].toString().length(), " "));
                    if (!batchMode) {
                        leftPad = " | ";
                    } else {
                        leftPad = " ";
                    }
                }
                if (!batchMode) {
                    builder.append(" |\n").append(getChar(rowWidth, "-"));
                }
                builder.append('\n');
            }

            return builder.toString();
        }

        private String getChar(final int count, final String character) {
            String spaces = "";
            for (int j = 0; j < count; j++) {
                spaces += character;
            }
            return spaces;
        }

        private int[] getColumnSizes() {
            final int[] sizes = new int[header.length];
            for (int i = 0; i < sizes.length; i++) {
                int min = header[i].length();
                for (final String[] row : rows) {
                    int rowLength = row[i].length();
                    if (rowLength > min) {
                        min = rowLength;
                    }
                }
                sizes[i] = min;
            }

            return sizes;
        }
    }

    public static class CounterMap<K> {

        private Map<K, AtomicInteger> counterMap = new HashMap<>();

        public CounterMap() {
            super();
        }

        public void increment(K key) {
            AtomicInteger integer = counterMap.get(key);
            if (integer == null) {
                integer = new AtomicInteger(0);
                counterMap.put(key, integer);
            }
            integer.incrementAndGet();
        }

        public int getCount(K key) {
            AtomicInteger integer = counterMap.get(key);
            if (integer == null) {
                return 0;
            }
            return integer.get();
        }

        public Set<K> keySet() {
            return counterMap.keySet();
        }
    }

}
