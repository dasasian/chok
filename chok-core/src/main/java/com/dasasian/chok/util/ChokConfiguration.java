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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;

public class ChokConfiguration implements Serializable {

    public static final String CHOK_CONFIGURATION_HOME = "chok.configuration.home";

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ChokConfiguration.class);
    private static final long serialVersionUID = 1L;
    protected final Properties properties;

    public ChokConfiguration(final String path) {
        String chokPropertyHome = System.getProperty(CHOK_CONFIGURATION_HOME, "");
        properties = PropertyUtil.loadProperties(chokPropertyHome + path);
    }

    ChokConfiguration() {
        properties = new Properties();
    }

    public String getProperty(final String key) {
        return getProperty(key, null);
    }

    String getProperty(final String key, @Nullable final String defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            if(defaultValue != null) {
                value = defaultValue;
            }
            else {
                throw new IllegalStateException("no property with key '" + key + "' found");
            }
        }
        return value;
    }

    void setProperty(String key, long value) {
        properties.setProperty(key, Long.toString(value));
    }

    @SuppressWarnings("unused")
    public boolean getBoolean(final String key) {
        return getBoolean(key, null);
    }

    public boolean getBoolean(final String key, @Nullable final Boolean defaultValue) {
        String defaultValueStr = (defaultValue != null) ? Boolean.toString(defaultValue) : null;

        return Boolean.parseBoolean(getProperty(key, defaultValueStr));
    }

    public int getInt(final String key) {
        return getInt(key, null);
    }

    public int getInt(final String key, final Integer defaultValue) {
        String defaultValueStr = (defaultValue != null) ? Integer.toString(defaultValue) : null;

        return Integer.parseInt(getProperty(key, defaultValueStr));
    }

    @SuppressWarnings("unused")
    public float getFloat(final String key) {
        return getFloat(key, null);
    }

    public float getFloat(final String key, final Float defaultValue) {
        String defaultValueStr = (defaultValue != null) ? Float.toString(defaultValue) : null;
        return Float.parseFloat(getProperty(key, defaultValueStr));
    }

    public Path getPath(final String key) {
        return Paths.get(getProperty(key));
    }

    public Class<?> getClass(final String key) {
        return getClass(key, null);
    }

    public Class<?> getClass(final String key, Class<?> defaultValue) {
        String defaultClassName = (defaultValue != null) ? defaultValue.getName() : null;

        final String className = getProperty(key, defaultClassName);
        return ClassUtil.forName(className, Object.class);
    }

    public Set<String> getKeys() {
        return properties.stringPropertyNames();
    }

}
