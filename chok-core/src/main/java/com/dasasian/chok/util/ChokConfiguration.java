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

import com.google.common.base.Optional;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

public class ChokConfiguration implements Serializable {

    @SuppressWarnings("unused")
    private final static Logger LOG = Logger.getLogger(ChokConfiguration.class);
    private static final long serialVersionUID = 1L;

    public static final String CHOK_CONFIGURATION_HOME = "chok.configuration.home";
    protected Properties properties;

    public ChokConfiguration(final String path) {
        String chokPropertyHome = System.getProperty(CHOK_CONFIGURATION_HOME, "");
        properties = PropertyUtil.loadProperties(chokPropertyHome + path);
    }

    public ChokConfiguration() {
        properties = new Properties();
    }

    public String getProperty(final String key) {
        return getProperty(key, Optional.<String>absent());
    }

    public String getProperty(final String key, final String defaultValue) {
        return getProperty(key, Optional.of(defaultValue));
    }

    public String getProperty(final String key, final Optional<String> defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            if (defaultValue.isPresent()) {
                value = defaultValue.get();
            }
            else {
                throw new IllegalStateException("no property with key '" + key + "' found");
            }
        }
        return value;
    }

    protected void setProperty(String key, long value) {
        properties.setProperty(key, Long.toString(value));
    }

    @SuppressWarnings("unused")
    public boolean getBoolean(final String key) {
        return getBoolean(key, Optional.<Boolean>absent());
    }

    public boolean getBoolean(final String key, boolean defaultValue) {
        return getBoolean(key, Optional.of(defaultValue));
    }

    public boolean getBoolean(final String key, final Optional<Boolean> defaultValue) {
        Optional<String> defaultValueStr = (defaultValue.isPresent()) ? Optional.of(Boolean.toString(defaultValue.get())) : Optional.<String>absent();

        return Boolean.parseBoolean(getProperty(key, defaultValueStr));
    }

    public int getInt(final String key) {
        return getInt(key, Optional.<Integer>absent());
    }

    public int getInt(final String key, final int defaultValue) {
        return getInt(key, Optional.of(defaultValue));
    }

    public int getInt(final String key, final Optional<Integer> defaultValue) {
        Optional<String> defaultValueStr = (defaultValue.isPresent()) ? Optional.of(Integer.toString(defaultValue.get())) : Optional.<String>absent();

        return Integer.parseInt(getProperty(key, defaultValueStr));
    }

    @SuppressWarnings("unused")
    public float getFloat(final String key) {
        return getFloat(key, Optional.<Float>absent());
    }

    public float getFloat(final String key, final float defaultValue) {
        return getFloat(key, Optional.of(defaultValue));
    }

    public float getFloat(final String key, final Optional<Float> defaultValue) {
        Optional<String> defaultValueStr = (defaultValue.isPresent()) ? Optional.of(Float.toString(defaultValue.get())) : Optional.<String>absent();
        return Float.parseFloat(getProperty(key, defaultValueStr));
    }

    public File getFile(final String key) {
        return new File(getProperty(key));
    }

    public Class<?> getClass(final String key) {
        return getClass(key, Optional.<Class<?>>absent());
    }

    public Class<?> getClass(final String key, Class<?> defaultValue) {
        return getClass(key, Optional.<Class<?>>of(defaultValue));

    }

    public Class<?> getClass(final String key, Optional<Class<?>> defaultValue) {
        Optional<String> defaultClassName = (defaultValue.isPresent()) ? Optional.of(defaultValue.get().getName()) : Optional.<String>absent();

        final String className = getProperty(key, defaultClassName);
        return ClassUtil.forName(className, Object.class);
    }

    public Set<String> getKeys() {
        return properties.stringPropertyNames();
    }

}
