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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Created by damith.chandrasekara on 6/15/15.
 */
public class ChokFileSystem {
    private Configuration conf;
    private final FileSystem fileSystem;

    public static ChokFileSystem get(URI indexUri, Configuration conf) throws IOException {
        return new ChokFileSystem(FileSystem.get(indexUri, conf), conf);
    }

    public static ChokFileSystem getLocal(Configuration conf) throws IOException {
        return new ChokFileSystem(FileSystem.getLocal(conf), conf);
    }

    public static ChokFileSystem getThrottled(URI uri, Configuration conf, ThrottledInputStream.ThrottleSemaphore throttleSemaphore) throws IOException {
        // todo fix
        return new ThrottledFileSystem(FileSystem.get(uri, conf), conf, throttleSemaphore);
    }


    ChokFileSystem(FileSystem fileSystem, Configuration conf) {
        this.fileSystem = fileSystem;
        this.conf = conf;
    }

    public boolean exists(URI uri) throws IOException {
        return fileSystem.exists(new Path(uri));
    }

    public Iterable<URI> list(URI uri) throws IOException, URISyntaxException {
        List<URI> uriList = Lists.newArrayList();
        for(FileStatus fileStatus : fileSystem.listStatus(new Path(uri), aPath -> !aPath.getName().startsWith("."))) {
            String shardPath = fileStatus.getPath().toString();
            // todo do I need this?
//            if (fileStatus.isDir() || shardPath.endsWith(".zip")) {
                uriList.add(ChokFileSystem.getURI(shardPath));
//            }
        }
        return uriList;
    }

    public boolean isDir(URI uri) throws IOException {
        return fileSystem.isDirectory(new Path(uri));
    }

    public long size(URI uri) throws IOException {
        return fileSystem.getFileStatus(new Path(uri)).getLen();
    }

    public boolean isFile(URI uri) throws IOException {
        return fileSystem.isFile(new Path(uri));
    }

    public void copyToLocalFile(URI from, URI to) throws IOException {
        fileSystem.copyToLocalFile(new Path(from), new Path(to));
    }

    public InputStream open(URI source) throws IOException {
        return open(source, conf.getInt("io.file.buffer.size", 4096));
    }

    public InputStream open(URI source, int bufferSize) throws IOException {
        return fileSystem.open(new Path(source), bufferSize);
    }

    public static URI getURI(String shardPath) throws URISyntaxException {
        return new URI(shardPath);
    }
}
