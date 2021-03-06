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

import com.dasasian.chok.util.ClassUtil;
import com.dasasian.chok.util.ZkConfiguration;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class RunClassCommand extends Command {

    private Method method;
    private String[] methodArgs;

    public RunClassCommand() {
        super("runclass", "<className>", "runs a custom class");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) throws Exception {
        CommandLineHelper.validateMinArguments(args, 2);
        final Class<?> clazz = ClassUtil.forName(args[1], Object.class);
        method = clazz.getMethod("main", args.getClass());
        if (method == null) {
            throw new IllegalArgumentException("class " + clazz.getName() + " doesn't have a main method");
        }
        methodArgs = Arrays.copyOfRange(args, 2, args.length);
    }

    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
        method.invoke(null, new Object[]{methodArgs});
    }

}
