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
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import com.google.common.collect.*;

import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Filter for Logging Events
 * <p>
 * Created by damith.chandrasekara on 8/29/15.
 */
public class TestLoggerEvaluatorFilter extends Filter<ILoggingEvent> {

    private Map<String, Level> logger2Level = Maps.newHashMap();
    private ListMultimap<String, ILoggingEvent> logger2Events = ArrayListMultimap.create();

    public synchronized FilterReply decide(ILoggingEvent event) {
        String loggerName = event.getLoggerName();
        if (logger2Level.containsKey(loggerName)) {
            logger2Events.put(loggerName, event);
            Level level = logger2Level.get(loggerName);
            if (!event.getLevel().isGreaterOrEqual(level)) {
                return FilterReply.DENY;
            }
        }
        return FilterReply.NEUTRAL;
    }

    public synchronized void setLoggerLevel(String loggerName, Level level) {
        logger2Level.put(loggerName, level);
    }


    public synchronized void resetLogger(String loggerName) {
        logger2Level.remove(loggerName);
        logger2Events.removeAll(loggerName);
    }

    public synchronized Stream<ILoggingEvent> getLoggerEvents(String loggerName, Predicate<ILoggingEvent> loggingEventPredicate) {
        return logger2Events.get(loggerName).stream()
                .filter(loggingEventPredicate);
    }

}
