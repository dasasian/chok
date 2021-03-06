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
package com.dasasian.chok;

import com.dasasian.chok.command.*;
import com.dasasian.chok.util.UtilModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Provides command line access to a Chok cluster.
 */
public class ChokMap extends CommandLineInterface {

    @Inject protected StartMapFileNodeCommand startMapFileNodeCommand;

    public void init() {
        addBaseCommands();
        addCommand(startMapFileNodeCommand);
    }

    public static void main(String[] args) throws Exception {
        Injector injector = Guice.createInjector(new UtilModule());
        ChokMap chok = injector.getInstance(ChokMap.class);
        chok.init();
        chok.execute(args);
    }

}
