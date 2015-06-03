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
package com.dasasian.chok.protocol.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Version of a cluster or distribution (depending if loaded from jar or from
 * the cluster itself).
 */
public class Version implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Version.class);
    /**
     * The version of Chok.
     */
    private final String number;
    private final String revision;
    private final String createdBy;
    private final String compileTime;

    public Version(String number, String revision, String createdBy, String compileTime) {
        this.number = number;
        this.revision = revision;
        this.createdBy = createdBy;
        this.compileTime = compileTime;
    }

    public static Version readFromJar() {
        String jar = findContainingJar(Version.class);
        String number = "Unknown";
        String revision = "Unknown";
        String createdBy = "Unknown";
        String compileTime = "Unknown";
        if (jar != null) {
            LOG.debug("load version info from '" + jar + "'");
            final Manifest manifest = getManifest(jar);

            final Map<String, Attributes> attrs = manifest.getEntries();
            Attributes attr = attrs.get("com.dasasian.chok");
            if (attr != null) {
                number = attr.getValue("Implementation-Version");
                revision = attr.getValue("Git-Revision");
                createdBy = attr.getValue("Built--By");
                compileTime = attr.getValue("Compile-Time");
            }
        }
        return new Version(number, revision, createdBy, compileTime);
    }

    private static String findContainingJar(Class<?> clazz) {
        ClassLoader loader = clazz.getClassLoader();
        String className = clazz.getName().replaceAll("\\.", "/") + ".class";
        try {
            for (Enumeration<URL> enumeration = loader.getResources(className); enumeration.hasMoreElements(); ) {
                URL url = enumeration.nextElement();
                if ("jar".equals(url.getProtocol())) {
                    String path = url.getPath();
                    if (path.startsWith("file:")) {
                        path = path.substring("file:".length());
                    }
                    path = URLDecoder.decode(path, "UTF-8");
                    return path.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private static Manifest getManifest(String jar) {
        try {
            final JarFile jarFile = new JarFile(jar);
            return jarFile.getManifest();
        } catch (Exception e) {
            throw new RuntimeException("could not load manifest from jar " + jar);
        }
    }

    public String getNumber() {
        return number;
    }

    public String getRevision() {
        return revision;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public String getCompileTime() {
        return compileTime;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((number == null) ? 0 : number.hashCode());
        result = prime * result + ((revision == null) ? 0 : revision.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Version other = (Version) obj;
        if (number == null) {
            if (other.number != null)
                return false;
        } else if (!number.equals(other.number))
            return false;
        if (revision == null) {
            if (other.revision != null)
                return false;
        } else if (!revision.equals(other.revision))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return getNumber() + " | " + getRevision() + " | " + getCompileTime() + " | by " + getCreatedBy();
    }
}
