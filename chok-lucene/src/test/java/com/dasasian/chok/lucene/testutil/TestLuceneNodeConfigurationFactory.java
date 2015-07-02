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

import com.dasasian.chok.lucene.DefaultSearcherFactory;
import com.dasasian.chok.lucene.ISearcherFactory;
import com.dasasian.chok.lucene.LuceneNodeConfiguration;
import com.dasasian.chok.lucene.LuceneNodeConfigurationLoader;
import com.dasasian.chok.testutil.NodeConfigurationFactory;
import com.dasasian.chok.util.NodeConfiguration;
import org.junit.rules.TemporaryFolder;

/**
 * User: damith.chandrasekara
 * Date: 7/4/13
 */
public class TestLuceneNodeConfigurationFactory implements NodeConfigurationFactory {

    private final TemporaryFolder temporaryFolder;

    public TestLuceneNodeConfigurationFactory(TemporaryFolder temporaryFolder) {
        this.temporaryFolder = temporaryFolder;
    }

    @Override
    public NodeConfiguration getConfiguration() {
        return getConfiguration(0.5f);
    }

    @Override
    public NodeConfiguration getConfiguration(int nodeStartPort) {
        return getConfiguration(DefaultSearcherFactory.class, 0.5f);
    }

    public NodeConfiguration getConfiguration(float timeoutPercentage) {
        return getConfiguration(DefaultSearcherFactory.class, timeoutPercentage);
    }

    public LuceneNodeConfiguration getConfiguration(Class<? extends ISearcherFactory> searcherFactoryClass, float timeoutPercentage) {
        return getConfiguration(20000, searcherFactoryClass, timeoutPercentage);
    }

    public LuceneNodeConfiguration getConfiguration(int startPort, Class<? extends ISearcherFactory> searcherFactoryClass, float timeoutPercentage) {
        try {
            return LuceneNodeConfigurationLoader.createConfiguration(temporaryFolder.newFolder().toPath(), startPort, searcherFactoryClass, timeoutPercentage);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
