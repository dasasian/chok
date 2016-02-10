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
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * Test for TestLoggerWatcher
 *
 * Created by damith.chandrasekara on 9/5/15.
 */
public class TestLoggerWatcherTest {

    private static final Logger LOG = LoggerFactory.getLogger(TestLoggerWatcherTest.class);

    @Rule
    public TestLoggerWatcher logOffNoMethodNameWatcher = TestLoggerWatcher.logOff(TestLoggerWatcherTest.class);

    @Rule
    public TestLoggerWatcher logOffWatcher = TestLoggerWatcher.logOff(TestLoggerWatcherTest.class, "testLogOff");

    @Rule
    public TestLoggerWatcher logErrorsWatcher = TestLoggerWatcher.logErrors(TestLoggerWatcherTest.class, "testLogErrors");


    @Test
    public void testLogOffNoMethodName() {
        assertThat("Got 0 event(s) above warn", logOffNoMethodNameWatcher.getLogEventCount(event -> event.getLevel().isGreaterOrEqual(Level.WARN)), is(equalTo(0)));

        assertThat("Got 0 warning(s)", logOffNoMethodNameWatcher.getLogEventCount(event -> event.getLevel().equals(Level.WARN)), is(equalTo(0)));
        LOG.warn("Logging warning");
        assertThat("Got 1 warning(s)", logOffNoMethodNameWatcher.getLogEventCount(event -> event.getLevel().equals(Level.WARN)), is(equalTo(1)));

        assertThat("Got 0 error", logOffNoMethodNameWatcher.getLogEventCount(event -> event.getLevel().equals(Level.ERROR)), is(equalTo(0)));
        LOG.error("Logging error");
        assertThat("Got 1 error", logOffNoMethodNameWatcher.getLogEventCount(event -> event.getLevel().equals(Level.ERROR)), is(equalTo(1)));

        assertThat("Got 2 event(s) above warn", logOffNoMethodNameWatcher.getLogEventCount(event -> event.getLevel().isGreaterOrEqual(Level.WARN)), is(equalTo(2)));
    }

    @Test
    public void testLogOff() {
        assertThat("Got 0 event(s) above warn", logOffWatcher.getLogEventCount(event -> event.getLevel().isGreaterOrEqual(Level.WARN)), is(equalTo(0)));

        assertThat("Got 0 warning(s)", logOffWatcher.getLogEventCount(event -> event.getLevel().equals(Level.WARN)), is(equalTo(0)));
        LOG.warn("Logging warning");
        assertThat("Got 1 warning(s)", logOffWatcher.getLogEventCount(event -> event.getLevel().equals(Level.WARN)), is(equalTo(1)));

        assertThat("Got 0 error", logOffWatcher.getLogEventCount(event -> event.getLevel().equals(Level.ERROR)), is(equalTo(0)));
        LOG.error("Logging error");
        assertThat("Got 1 error", logOffWatcher.getLogEventCount(event -> event.getLevel().equals(Level.ERROR)), is(equalTo(1)));

        assertThat("Got 2 event(s) above warn", logOffWatcher.getLogEventCount(event -> event.getLevel().isGreaterOrEqual(Level.WARN)), is(equalTo(2)));
    }

    @Test
    public void testLogErrors() {
        assertThat("Got 0 event(s) above warn", logErrorsWatcher.getLogEventCount(event -> event.getLevel().isGreaterOrEqual(Level.WARN)), is(equalTo(0)));

        assertThat("Got 0 warning(s)", logErrorsWatcher.getLogEventCount(event -> event.getLevel().equals(Level.WARN)), is(equalTo(0)));
        LOG.warn("Logging warning");
        assertThat("Got 1 warning(s)", logErrorsWatcher.getLogEventCount(event -> event.getLevel().equals(Level.WARN)), is(equalTo(1)));

        assertThat("Got 0 error", logErrorsWatcher.getLogEventCount(event -> event.getLevel().equals(Level.ERROR)), is(equalTo(0)));
        LOG.error("Logging error");
        assertThat("Got 1 error", logErrorsWatcher.getLogEventCount(event -> event.getLevel().equals(Level.ERROR)), is(equalTo(1)));

        assertThat("Got 2 event(s) above warn", logErrorsWatcher.getLogEventCount(event -> event.getLevel().isGreaterOrEqual(Level.WARN)), is(equalTo(2)));
    }

}