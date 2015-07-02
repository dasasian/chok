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
package com.dasasian.chok.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

/**
 * Created by damith.chandrasekara on 6/15/15.
 */
public class ThrottledFileSystem implements ChokFileSystem {
    // todo fix

    private final ChokFileSystem chokFileSystem;
    private final ThrottledInputStream.ThrottleSemaphore throttleSemaphore;

    public ThrottledFileSystem(ChokFileSystem chokFileSystem, ThrottledInputStream.ThrottleSemaphore throttleSemaphore) {
        this.chokFileSystem = chokFileSystem;
        this.throttleSemaphore = throttleSemaphore;
    }

    @Override
    public boolean exists(URI uri) throws IOException {
        return chokFileSystem.exists(uri);
    }

    @Override
    public Iterable<URI> list(URI uri) throws IOException, URISyntaxException {
        return chokFileSystem.list(uri);
    }

    @Override
    public boolean isDir(URI uri) throws IOException {
        return chokFileSystem.isDir(uri);
    }

    @Override
    public boolean isFile(URI uri) throws IOException {
        return chokFileSystem.isFile(uri);
    }

    @Override
    public void copyToLocalFile(URI from, Path to) throws IOException {
        chokFileSystem.copyToLocalFile(from, to);
    }

    @Override
    public InputStream open(URI uri) throws IOException {
        return new ThrottledInputStream(chokFileSystem.open(uri), throttleSemaphore);
    }

    @Override
    public long lastModified(URI uri) throws IOException {
        return chokFileSystem.lastModified(uri);
    }

    @Override
    public long size(URI uri) throws IOException {
        return chokFileSystem.size(uri);
    }

}
