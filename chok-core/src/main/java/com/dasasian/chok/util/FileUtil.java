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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileUtil {

    public static final FilenameFilter VISIBLE_FILES_FILTER = (dir, name) -> !name.startsWith(".");

    public static final DirectoryStream.Filter<Path> VISIBLE_PATHS_FILTER = path -> !path.getFileName().toString().startsWith(".");

    private final static Logger LOG = LoggerFactory.getLogger(FileUtil.class);
    private static final int BUFFER = 4096;

    public static void deleteFolder(File folder) {
        try {
            FileUtils.forceDelete(folder);
        } catch (FileNotFoundException ignore) {
        } catch (IOException e) {
            throw new RuntimeException("could not delete folder '" + folder + "'", e);
        }
    }

    /**
     * Simply unzips the content from the source zip to the target folder. The
     * first level folder of the zip content is removed.
     *
     * @param sourceZip    the path to the source zip file, hadoop's IO services are used to
     *                     open this path
     * @param targetFolder The directory that the zip file will be unpacked into
     * @param fileSystem   the hadoop file system object to use to open
     *                     <code>sourceZip</code>
     * @param localSpool   If true, the zip file is copied to the local file system before
     *                     being unzipped. The name used is <code>targetFolder.zip</code>. If
     *                     false, the unzip is streamed.
     */
    public static void unzip(final URI sourceZip, final Path targetFolder, final ChokFileSystem fileSystem, final boolean localSpool) {
        try {
            if (localSpool) {
                Files.createDirectories(targetFolder);
                final Path shardZipLocal = Paths.get(targetFolder.toString() + ".zip");
                if (Files.exists(shardZipLocal)) {
                    // make sure we overwrite cleanly
                    Files.delete(shardZipLocal);
                }
                try {
                    fileSystem.copyToLocalFile(sourceZip, shardZipLocal);
                    FileUtil.unzip(shardZipLocal, targetFolder);
                } finally {
                    Files.delete(shardZipLocal);
                }
            } else {
                try(final ZipInputStream zis = new ZipInputStream(fileSystem.open(sourceZip))) {
                    unzip(zis, targetFolder);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("unable to expand upgrade files for " + sourceZip + " to " + targetFolder, e);
        }
    }

    /**
     * Simply unzips the content from the source zip to the target folder. The
     * first level folder of the zip content is removed.
     * @param sourceZip the source zip
     * @param targetFolder the target folder
     */
    protected static void unzip(final Path sourceZip, final Path targetFolder) {
        try(final ZipInputStream zis = new ZipInputStream(Files.newInputStream(sourceZip))) {
            LOG.debug("Extracting zip file '" + sourceZip + "' to '" + targetFolder + "'");
            unzip(zis, targetFolder);
        } catch (final Exception e) {
            throw new RuntimeException("unable to expand upgrade files for " + sourceZip + " to " + targetFolder, e);
        }
    }



    /**
     * Unpack a zip stream to a directory usually called by {@link #unzip(Path, Path)} or {@link #unzip(URI, Path, ChokFileSystem, boolean)}.
     *
     * @param zis          Zip data strip to unpack
     * @param targetFolder The folder to unpack to. This directory and path is created if needed.
     * @throws IOException If there is an error.
     */
    public static void unzip(final ZipInputStream zis, final Path targetFolder) throws IOException {
        ZipEntry entry;

        Files.createDirectories(targetFolder);
        while ((entry = zis.getNextEntry()) != null) {
            LOG.debug("Extracting:   " + entry);
            // we need to remove the first element of the path since the
            // folder was compressed but we only want the folders content
            final String entryPath = entry.getName();
            final int indexOf = entryPath.indexOf("/");
            final String cleanUpPath = entryPath.substring(indexOf + 1, entryPath.length());
            final Path targetFile = targetFolder.resolve(cleanUpPath);
            if (entry.isDirectory()) {
                Files.createDirectories(targetFile);
            }
            else {
                if (!Files.exists(targetFile.getParent())) {
                    Files.createDirectories(targetFile.getParent());
                }
                Files.copy(zis, targetFile);
            }
        }

    }

//    public static void zip(final File inputFolder, final File outputFile) throws IOException {
//        final FileOutputStream fileWriter = new FileOutputStream(outputFile);
//        final ZipOutputStream zip = new ZipOutputStream(fileWriter);
//        addFolderToZip("", inputFolder, zip);
//        zip.flush();
//        zip.close();
//    }
//
//    private static void addFolderToZip(final String path, final File folder, final ZipOutputStream zip) throws IOException {
//        final String zipEnry = path + (path.equals("") ? "" : File.separator) + folder.getName();
//        final File[] listFiles = folder.listFiles();
//        for (final File file : listFiles) {
//            if (file.isDirectory()) {
//                addFolderToZip(zipEnry, file, zip);
//            } else {
//                addFileToZip(zipEnry, file, zip);
//            }
//        }
//    }
//
//    private static void addFileToZip(final String path, final File file, final ZipOutputStream zip) throws IOException {
//        final byte[] buffer = new byte[1024];
//        int read;
//        try (FileInputStream in = new FileInputStream(file)) {
//            final String zipEntry = path + File.separator + file.getName();
//            LOG.debug("add zip entry: " + zipEntry);
//            zip.putNextEntry(new ZipEntry(zipEntry));
//            while ((read = in.read(buffer)) > -1) {
//                zip.write(buffer, 0, read);
//            }
//        }
//    }

//    public static void unzipInDfs(ChokFileSystem fileSystem, final URI source, final URI target) {
//        try {
//            FSDataInputStream dfsInputStream = fileSystem.open(source);
//            fileSystem.mkdirs(target);
//            final ZipInputStream zipInputStream = new ZipInputStream(dfsInputStream);
//            ZipEntry entry;
//
//            while ((entry = zipInputStream.getNextEntry()) != null) {
//                final String entryPath = entry.getName();
//                final int indexOf = entryPath.indexOf("/");
//                final String cleanUpPath = entryPath.substring(indexOf + 1, entryPath.length());
//                Path path = target;
//                if (!cleanUpPath.equals("")) {
//                    path = new Path(target, cleanUpPath);
//                }
//                LOG.info("Extracting: " + entry + " to " + path);
//                if (entry.isDirectory()) {
//                    fileSystem.mkdirs(path);
//                } else {
//                    int count;
//                    final byte data[] = new byte[4096];
//                    FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
//                    while ((count = zipInputStream.read(data, 0, 4096)) != -1) {
//                        fsDataOutputStream.write(data, 0, count);
//                    }
//                    fsDataOutputStream.flush();
//                    fsDataOutputStream.close();
//                }
//            }
//            zipInputStream.close();
//        } catch (final Exception e) {
//            LOG.error("can not open zip file", e);
//            throw new RuntimeException("unable to expand upgrade files", e);
//        }
//
//    }

}
