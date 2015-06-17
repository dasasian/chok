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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by damith.chandrasekara on 6/15/15.
 */
public class ThrottledFileSystem extends ChokFileSystem {
    // todo fix

    private final ThrottledInputStream.ThrottleSemaphore throttleSemaphore;

    public ThrottledFileSystem(FileSystem fileSystem, Configuration configuration, ThrottledInputStream.ThrottleSemaphore throttleSemaphore) {
        super(fileSystem, configuration);
        this.throttleSemaphore = throttleSemaphore;
    }

    @Override
    public InputStream open(URI path, int bufferSize) throws IOException {
        ThrottledInputStream throttledInputStream = new ThrottledInputStream(super.open(path, bufferSize), throttleSemaphore);
        return throttledInputStream;
    }
}
