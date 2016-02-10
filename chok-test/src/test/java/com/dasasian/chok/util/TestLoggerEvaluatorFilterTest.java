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
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.Test;

import static ch.qos.logback.classic.Level.*;
import static ch.qos.logback.core.spi.FilterReply.DENY;
import static ch.qos.logback.core.spi.FilterReply.NEUTRAL;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * Test for TestLoggingEvaluatorFilter
 *
 * Created by damith.chandrasekara on 9/5/15.
 */
public class TestLoggerEvaluatorFilterTest {

    public static final String LOGGER_NAME = TestLoggerEvaluatorFilter.class.getName();

    @Test
    public void testDecide() {
        TestLoggerEvaluatorFilter testLoggerEvaluatorFilter = new TestLoggerEvaluatorFilter();
        assertThat(testLoggerEvaluatorFilter.getLoggerEvents(LOGGER_NAME, e -> true).count(), is(0L));

        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(TRACE)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(DEBUG)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(INFO)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(WARN)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(ERROR)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.getLoggerEvents(LOGGER_NAME, e -> true).count(), is(0L));

        testLoggerEvaluatorFilter.setLoggerLevel(LOGGER_NAME, TRACE);
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(TRACE)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(DEBUG)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(INFO)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(WARN)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(ERROR)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.getLoggerEvents(LOGGER_NAME, e -> true).count(), is(5L));

        testLoggerEvaluatorFilter.setLoggerLevel(LOGGER_NAME, DEBUG);
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(TRACE)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(DEBUG)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(INFO)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(WARN)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(ERROR)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.getLoggerEvents(LOGGER_NAME, e -> true).count(), is(10L));

        testLoggerEvaluatorFilter.setLoggerLevel(LOGGER_NAME, INFO);
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(TRACE)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(DEBUG)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(INFO)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(WARN)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(ERROR)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.getLoggerEvents(LOGGER_NAME, e -> true).count(), is(15L));

        testLoggerEvaluatorFilter.setLoggerLevel(LOGGER_NAME, WARN);
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(TRACE)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(DEBUG)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(INFO)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(WARN)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(ERROR)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.getLoggerEvents(LOGGER_NAME, e -> true).count(), is(20L));

        testLoggerEvaluatorFilter.setLoggerLevel(LOGGER_NAME, ERROR);
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(TRACE)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(DEBUG)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(INFO)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(WARN)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(ERROR)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.getLoggerEvents(LOGGER_NAME, e -> true).count(), is(25L));

        testLoggerEvaluatorFilter.setLoggerLevel(LOGGER_NAME, Level.OFF);
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(TRACE)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(DEBUG)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(INFO)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(WARN)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(ERROR)), is(DENY));
        assertThat(testLoggerEvaluatorFilter.getLoggerEvents(LOGGER_NAME, e -> true).count(), is(30L));

        testLoggerEvaluatorFilter.resetLogger(LOGGER_NAME);
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(TRACE)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(DEBUG)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(INFO)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(WARN)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.decide(getLoggingEvent(ERROR)), is(NEUTRAL));
        assertThat(testLoggerEvaluatorFilter.getLoggerEvents(LOGGER_NAME, e -> true).count(), is(0L));
    }

    private LoggingEvent getLoggingEvent(Level info) {
        LoggingEvent loggingEvent = new LoggingEvent();
        loggingEvent.setLoggerName(LOGGER_NAME);
        loggingEvent.setLevel(info);
        return loggingEvent;
    }

}