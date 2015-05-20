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

import com.dasasian.chok.master.Master;
import com.dasasian.chok.operation.master.CheckIndicesOperation;
import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.util.ZkConfiguration;

/**
 * User: damith.chandrasekara
 * Date: 7/7/13
 */
public class IndexAutoRepairCommand extends ProtocolCommand {

    private boolean enable;

    public IndexAutoRepairCommand() {
        super("indexAutoRepair", "enable/disable", "Enable or Disable index auto repair");
    }

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
        CommandLineHelper.validateMinArguments(args, 2);
        String enableDisable = args[1];
        if ("enable".equalsIgnoreCase(enableDisable)) {
            enable = true;
        } else if ("disable".equalsIgnoreCase(enableDisable)) {
            enable = false;
        }
        throw new RuntimeException("Invalid value for enable/disable:" + enableDisable);
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        if(enable) {
            protocol.disableIndexAutoRepair();
            System.out.println("Enabled index auto repair");
            // add check indices operation
            System.out.println("Checking indexes");
            protocol.addMasterOperation(new CheckIndicesOperation());
        }
        else {
            protocol.enableIndexAutoRepair();
            System.out.println("Disabled index auto repair");
        }
    }

}
