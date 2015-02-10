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
package com.dasasian.chok.testutil.server.simpletest;

import com.dasasian.chok.util.FileUtil;
import com.google.common.io.Files;

import java.io.File;

public class SimpleTestResources {

    private static File getInvalidIndex() {
        File index = Files.createTempDir();
        FileUtil.deleteFolder(index);
        if (!index.mkdir()) {
            throw new RuntimeException("Unable to create index dir " + index.getAbsoluteFile());
        }
        for (String shardName : new String[]{"a", "b", "c"}) {
            File shard = new File(index, shardName);
            if (!shard.mkdir()) {
                throw new RuntimeException("Unable to create index dir " + shard.getAbsoluteFile());
            }
        }
        return index;
    }

    public final static File INVALID_INDEX = getInvalidIndex();

    /**
     * The shards will be created at run time.
     */
    public final static File EMPTY1_INDEX = Files.createTempDir();
    /**
     * The shards will be created at run time.
     */
    public final static File EMPTY2_INDEX = Files.createTempDir();

}
