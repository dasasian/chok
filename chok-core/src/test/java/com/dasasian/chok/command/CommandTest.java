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

import com.dasasian.chok.protocol.metadata.IndexMetaData;
import com.dasasian.chok.testutil.AbstractZkTest;
import org.junit.Test;

// todo break this up into individual tests for each command class
public class CommandTest extends AbstractZkTest {

    @Test
    public void testShowStructure() throws Exception {
        execute(new ShowStructureCommand(), "showStructure");
        execute(new ShowStructureCommand(), "showStructure", "-d");
    }

    @Test(expected = Exception.class)
    public void testShowErrorsWithIndexNotExist() throws Exception {
        execute(new ListErrorsCommand(), "showStructure", "a");
    }

    // todo find out why this breaks SleepClientTest
//    @Test
    public void testListIndexesWithUnreachableIndex_CHOK_76() throws Exception {
        IndexMetaData indexMD = new IndexMetaData("indexABC", "hdfs://localhost:8020/unreachableIndex", 1);
        protocol.publishIndex(indexMD);
        execute(new ListIndicesCommand(), indexMD.getName());
    }

    private void execute(Command command, String... args) throws Exception {
        command.parseArguments(zk.getZkConfiguration(), args);
        command.execute(zk.getZkConfiguration());
    }

}
