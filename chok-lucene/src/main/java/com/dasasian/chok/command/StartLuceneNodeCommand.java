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

import com.dasasian.chok.lucene.LuceneNodeConfigurationLoader;
import com.dasasian.chok.lucene.LuceneServer;
import com.dasasian.chok.node.IContentServer;
import com.dasasian.chok.node.Node;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.ChokFileSystem;
import com.dasasian.chok.util.NodeConfiguration;
import com.dasasian.chok.util.ZkConfiguration;
import com.google.inject.Inject;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class StartLuceneNodeCommand extends ProtocolCommand {

    private final ChokFileSystem.Factory chokFileSystemFactory;
    private NodeConfiguration nodeConfiguration;
    private IContentServer server = null;

    @Inject
    public StartLuceneNodeCommand(ChokFileSystem.Factory chokFileSystemFactory) {
        super("startNode", "[-p <port number>]", "Starts a local node");
        this.chokFileSystemFactory = chokFileSystemFactory;
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
        Integer startPort = null;
        if (optionMap.containsKey("-p")) {
            startPort = Integer.parseInt(optionMap.get("-p"));
        }

        Path shardFolder = null;
        if (optionMap.containsKey("-f")) {
            shardFolder = Paths.get(optionMap.get("-f"));
        }

        try {
            nodeConfiguration = LuceneNodeConfigurationLoader.loadConfiguration(startPort, shardFolder);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        server = new LuceneServer();
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        final Node node = new Node(protocol, nodeConfiguration, server, chokFileSystemFactory);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                node.shutdown();
            }
        });
        node.join();
    }

}
