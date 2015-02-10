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
package com.dasasian.chok.command;

import com.dasasian.chok.node.monitor.MetricLogger;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.ZkConfiguration;

import java.util.Arrays;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class LogMetricsCommand extends ProtocolCommand {

    public LogMetricsCommand() {
        super("logMetrics", "[sysout|log4j]", "Subscribes to the Metrics updates and logs them to log file or console");
    }

    private MetricLogger.OutputType _outputType;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        if (args.length < 2) {
            throw new IllegalArgumentException("no output type specified");
        }
        if (parseType(args[1]) == null) {
            throw new IllegalArgumentException("need to specify one of " + Arrays.asList(MetricLogger.OutputType.values()) + " as output type");
        }
        _outputType = parseType(args[1]);

    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        new MetricLogger(_outputType, protocol).join();
    }

    private MetricLogger.OutputType parseType(String typeString) {
        for (MetricLogger.OutputType outputType : MetricLogger.OutputType.values()) {
            if (typeString.equalsIgnoreCase(outputType.name())) {
                return outputType;
            }
        }
        return null;
    }

}
