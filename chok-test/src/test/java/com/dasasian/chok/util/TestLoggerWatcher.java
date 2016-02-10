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
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.google.common.collect.ImmutableSet;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * Rule used to turn off logging for tests
 * Created by damith.chandrasekara on 8/27/15.
 */
public class TestLoggerWatcher extends TestWatcher {

    //name of the method to suspend logging for
    private final Set<String> methodNames;
    private final String loggerName;
    private String currentMethodName = null;

    private Level loggingLevel;
    private TestLoggerEvaluatorFilter testLoggerEvaluatorFilter;

    public static TestLoggerWatcher logOff(Class clazz) {
        return new TestLoggerWatcher(clazz.getName(), Level.OFF, null);
    }

    public static TestLoggerWatcher logOff(Class clazz, String... testMethodNames) {
        return new TestLoggerWatcher(clazz.getName(), Level.OFF, testMethodNames);
    }

    public static TestLoggerWatcher logErrors(Class clazz) {
        return new TestLoggerWatcher(clazz.getName(), Level.ERROR, null);
    }

    public static TestLoggerWatcher logErrors(Class clazz, String... testMethodNames) {
        return new TestLoggerWatcher(clazz.getName(), Level.ERROR, testMethodNames);
    }

    public static TestLoggerWatcher logLevel(Class clazz, Level loggingLevel, String... testMethodNames) {
        return new TestLoggerWatcher(clazz.getName(), loggingLevel, testMethodNames);
    }

    protected TestLoggerWatcher(String loggerName, Level loggingLevel, String... methodNames) {
        this.loggerName = loggerName;
        this.loggingLevel = loggingLevel;
        this.methodNames = methodNames != null ? ImmutableSet.copyOf(methodNames) : null;
    }

    @SuppressWarnings("unused")
    public List<String> getLogMessages() {
        return getLogMessages(event -> true);
    }

    @SuppressWarnings("unused")
    public List<String> getLogMessages(Predicate<ILoggingEvent> loggingEventPredicate) {
        return testLoggerEvaluatorFilter.getLoggerEvents(loggerName, loggingEventPredicate)
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unused")
    public int getLogEventCount(Predicate<ILoggingEvent> loggingEventPredicate) {
        return (int) testLoggerEvaluatorFilter.getLoggerEvents(loggerName, loggingEventPredicate).count();
    }

    @Override
    public void starting(Description desc) {
        currentMethodName = desc.getMethodName();
        if (methodNames == null || methodNames.contains(currentMethodName)) {
            Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            Iterator<Appender<ILoggingEvent>> appenderIterator = logger.iteratorForAppenders();
            while (appenderIterator.hasNext()) {
                Appender<ILoggingEvent> appender = appenderIterator.next();
                testLoggerEvaluatorFilter = (TestLoggerEvaluatorFilter) appender.getCopyOfAttachedFiltersList().stream()
                        .filter(filter -> filter instanceof TestLoggerEvaluatorFilter)
                        .findFirst()
                        .get();

                testLoggerEvaluatorFilter.setLoggerLevel(loggerName, loggingLevel);
            }
//            logger.setLevel(loggingLevel);
        }
    }

    @Override
    public void finished(Description desc) {
        if (methodNames == null || methodNames.contains(currentMethodName)) {
            testLoggerEvaluatorFilter.resetLogger(loggerName);
        }
        currentMethodName = null;
    }

}
