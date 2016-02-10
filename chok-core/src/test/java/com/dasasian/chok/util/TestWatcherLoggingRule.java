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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.ImmutableSet;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

import java.util.Set;


/**
 * Rule used to turn off logging for tests
 * Created by damith.chandrasekara on 8/27/15.
 */
public class TestWatcherLoggingRule extends TestWatcher {

    //name of the method to suspend logging for
    private final Set<String> methodNames;

    private Level loggingLevel;

    private Logger logger;

    public TestWatcherLoggingRule(Class clazz, String... testMethodNames) {
        this(clazz.getName(), testMethodNames);
    }

    public TestWatcherLoggingRule(String loggerName, String... methodNames) {
        this.methodNames = ImmutableSet.copyOf(methodNames);
        this.logger = (Logger) LoggerFactory.getLogger(loggerName);
        this.loggingLevel = logger.getLevel();
    }

    @Override
    public void starting(Description desc) {
        if (methodNames.contains(desc.getMethodName())) {
            logger.setLevel(Level.OFF);
        }
    }

    @Override
    public void finished(Description desc) {
        logger.setLevel(loggingLevel);
    }
}
