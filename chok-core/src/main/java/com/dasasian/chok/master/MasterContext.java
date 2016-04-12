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
package com.dasasian.chok.master;

import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.MasterQueue;
import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.util.ChokFileSystem;

import java.io.IOException;
import java.net.URISyntaxException;

public class MasterContext {

    private final Master master;
    private final InteractionProtocol protocol;
    private final IDeployPolicy deployPolicy;
    private final MasterQueue masterQueue;
    private final ChokFileSystem.Factory chokFileSystemFactory;

    public MasterContext(InteractionProtocol protocol, Master master, IDeployPolicy deployPolicy, MasterQueue masterQueue, ChokFileSystem.Factory chokFileSystemFactory) {
        this.protocol = protocol;
        this.master = master;
        this.deployPolicy = deployPolicy;
        this.masterQueue = masterQueue;
        this.chokFileSystemFactory = chokFileSystemFactory;
    }

    public InteractionProtocol getProtocol() {
        return protocol;
    }

    public Master getMaster() {
        return master;
    }

    public IDeployPolicy getDeployPolicy() {
        return deployPolicy;
    }

    public MasterQueue getMasterQueue() {
        return masterQueue;
    }

    public ChokFileSystem getChokFileSystem(IndexMetaData indexMd) throws IOException, URISyntaxException {
        return chokFileSystemFactory.create(indexMd.getUri());
    }

}
