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
package com.dasasian.chok.lucene;

import com.dasasian.chok.util.ChokConfiguration;
import com.dasasian.chok.util.ClassUtil;
import com.dasasian.chok.util.NodeConfiguration;
import com.dasasian.chok.util.NodeConfigurationLoader;
import com.google.common.base.Optional;

import java.io.File;
import java.nio.file.Path;

/**
 * User: damith.chandrasekara
 * Date: 7/4/13
 */
public class LuceneNodeConfigurationLoader {

    public final static String SEARCHER_FACTORY_CLASS = "lucene.searcher.factory-class";
    public final static String COLLECTOR_TIMOUT_PERCENTAGE = "lucene.collector.timeout-percentage";
    public final static String SEARCHER_THREADPOOL_CORESIZE = "lucene.searcher.threadpool.core-size";
    public final static String SEARCHER_THREADPOOL_MAXSIZE = "lucene.searcher.threadpool.max-size";
    public final static String FILTER_CACHE_ENABLED = "lucene.filter.cache.enabled";
    public final static String FILTER_CACHE_MAXSIZE = "lucene.filter.cache.max-size";

    public static LuceneNodeConfiguration loadConfiguration() throws ClassNotFoundException {
        NodeConfiguration nodeConfiguration = NodeConfigurationLoader.loadConfiguration();
        return getLuceneNodeConfiguration(nodeConfiguration);
    }

    private static LuceneNodeConfiguration getLuceneNodeConfiguration(NodeConfiguration nodeConfiguration) {
        ChokConfiguration chokConfiguration = new ChokConfiguration("/chok.node.properties");

        Class<? extends ISearcherFactory> searcherFactoryClass = ClassUtil.forName(chokConfiguration.getProperty(SEARCHER_FACTORY_CLASS), ISearcherFactory.class);
        float timeoutPercentage = chokConfiguration.getFloat(COLLECTOR_TIMOUT_PERCENTAGE, 0.75f);
        assert timeoutPercentage >= 0 && timeoutPercentage <= 1;

        int threadPoolCoreSize = chokConfiguration.getInt(SEARCHER_THREADPOOL_CORESIZE, 25);
        int threadPoolMaxSize = chokConfiguration.getInt(SEARCHER_THREADPOOL_MAXSIZE, 100);
        boolean filterCacheEnabled = chokConfiguration.getBoolean(FILTER_CACHE_ENABLED, true);
        int filterCacheMaxSize = chokConfiguration.getInt(FILTER_CACHE_MAXSIZE, 1000);

        return new LuceneNodeConfiguration(nodeConfiguration, searcherFactoryClass, timeoutPercentage, threadPoolCoreSize, threadPoolMaxSize, filterCacheEnabled, filterCacheMaxSize);
    }

    public static NodeConfiguration loadConfiguration(Optional<Integer> overrideStartPort, Optional<Path> overrideShardFolder) throws ClassNotFoundException {
        NodeConfiguration nodeConfiguration = NodeConfigurationLoader.loadConfiguration(overrideStartPort, overrideShardFolder);
        return getLuceneNodeConfiguration(nodeConfiguration);
    }

    public static LuceneNodeConfiguration createConfiguration(Path file, int startPort, Class<? extends ISearcherFactory> searcherFactoryClass, float timeoutPercentage) throws ClassNotFoundException {
        NodeConfiguration nodeConfiguration = NodeConfigurationLoader.createConfiguration(startPort, file);
        return new LuceneNodeConfiguration(nodeConfiguration, searcherFactoryClass, timeoutPercentage, 25, 100, true, 1000);
    }
}
