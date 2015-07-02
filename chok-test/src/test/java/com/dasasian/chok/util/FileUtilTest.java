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

import com.dasasian.chok.testutil.AbstractTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.channels.Pipe.SinkChannel;
import java.nio.channels.Pipe.SourceChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipInputStream;

import static org.junit.Assert.assertTrue;

public class FileUtilTest extends AbstractTest {

    public static final String INDEX_TXT = "index.txt";
    protected final Path testZipFile = Paths.get(FileUtilTest.class.getResource("/test.zip").getFile());

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void testUnzipFileFile() throws IOException {
        // ChokFileSystem fileSystem = ChokFileSystem.get(configuration);
        Path targetFolder = temporaryFolder.newFolder("unpacked1").toPath();
        FileUtil.unzip(testZipFile, targetFolder);

        Path segment = targetFolder.resolve(INDEX_TXT);
        assertTrue("Unzipped local zip directly to target", Files.exists(segment));
    }

    @Test
    public void testUnzipPathFileChokFileSystemBoolean() throws IOException, URISyntaxException {
        ChokFileSystem fileSystem = new HDFSChokFileSystem();

        // Test the unspooled case
        Path targetFolder = temporaryFolder.newFolder("unpacked2").toPath();
        URI zipPath = testZipFile.toUri();
        FileUtil.unzip(zipPath, targetFolder, fileSystem, false);
        Path segment = targetFolder.resolve(INDEX_TXT);
        assertTrue("Unzipped local zip directly to target", Files.exists(segment));

        // Test the spooled case

        targetFolder = temporaryFolder.newFolder("unpacked3").toPath();
        zipPath = testZipFile.toUri();
        FileUtil.unzip(zipPath, targetFolder, fileSystem, true);
        segment = targetFolder.resolve(INDEX_TXT);
        assertTrue("Unzipped spooled local zip to target", Files.exists(segment));

    }

    @Test
    public void testUnzipZipInputStreamFile() throws IOException {
        // Test on an unseekable input steam
        Path targetFolder = temporaryFolder.newFolder("unpacked4").toPath();

        final Pipe zipPipe = Pipe.open();
        final SinkChannel sink = zipPipe.sink();
        final SourceChannel source = zipPipe.source();
        final InputStream sourceIn = Channels.newInputStream(source);
        final OutputStream sourceOut = Channels.newOutputStream(sink);
        final ZipInputStream zis = new ZipInputStream(sourceIn);
        final InputStream fis = Files.newInputStream(testZipFile);
        final AtomicBoolean failed = new AtomicBoolean(false);
        // Write the zip data to the pipe a byte at a time to worst case the unzip
        // process
        Thread writer = new Thread() {
            @Override
            public void run() {
                try {
                    int b;
                    while ((b = fis.read()) >= 0) {
                        sourceOut.write(b);
                    }
                } catch (IOException e) {
                    System.err.println("shard transfer via pipe failed: " + e);
                    e.printStackTrace(System.err);
                    failed.set(true);
                } finally {
                    try {
                        fis.close();
                    } catch (IOException ignore) {
                        // ignore
                    }
                    try {
                        sourceOut.close();
                    } catch (IOException ignore) {
                        // ignore
                    }
                }
            }
        };
        writer.start();
        FileUtil.unzip(zis, targetFolder);
        Path segment = targetFolder.resolve(INDEX_TXT);
        assertTrue("Unzipped streamed zip to target", Files.exists(segment));
    }

}
