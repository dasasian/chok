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
package com.dasasian.chok.lucene.testutil;

import com.dasasian.chok.testutil.TestIndex;

public class LuceneTestResources {

    public final static TestIndex INDEX1 = new TestIndex(LuceneTestResources.class.getResource("/testIndexA"));
    public final static TestIndex INDEX2 = new TestIndex(LuceneTestResources.class.getResource("/testIndexB"));

    //public final static File UNZIPPED_INDEX = new File(LuceneTestResources.class.getResource("/testIndexC").getFile());
    //public final static File INVALID_INDEX = new File(LuceneTestResources.class.getResource("/testIndexInvalid").getFile());

}
