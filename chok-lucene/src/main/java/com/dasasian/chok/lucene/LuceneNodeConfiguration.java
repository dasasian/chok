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

import com.dasasian.chok.util.NodeConfiguration;

/**
 * User: damith.chandrasekara
 * Date: 7/4/13
 */
public class LuceneNodeConfiguration extends NodeConfiguration {

    private final Class<? extends ISearcherFactory> searcherFactorClass;
    private final float timeoutPercentage;
    private final int threadPoolCoreSize;
    private final int threadPoolMaxSize;
    private final boolean filterCacheEnabled;
    private final int filterCacheMaxSize;

    public LuceneNodeConfiguration(NodeConfiguration nodeConfiguration, Class<? extends ISearcherFactory> searcherFactorClass, float timeoutPercentage, int threadPoolCoreSize, int threadPoolMaxSize, boolean filterCacheEnabled, int filterCacheMaxSize) {
        super(nodeConfiguration.getStartPort(), nodeConfiguration.getShardFolder(), nodeConfiguration.getShardDeployThrottle(), nodeConfiguration.getMonitorClass(), nodeConfiguration.getRpcHandlerCount(), nodeConfiguration.getReloadCheckInterval());
        this.searcherFactorClass = searcherFactorClass;
        this.timeoutPercentage = timeoutPercentage;
        this.threadPoolCoreSize = threadPoolCoreSize;
        this.threadPoolMaxSize = threadPoolMaxSize;
        this.filterCacheEnabled = filterCacheEnabled;
        this.filterCacheMaxSize = filterCacheMaxSize;
    }

    public Class<? extends ISearcherFactory> getSearcherFactorClass() {
        return searcherFactorClass;
    }

    public float getTimeoutPercentage() {
        return timeoutPercentage;
    }

    public int getThreadPoolCoreSize() {
        return threadPoolCoreSize;
    }

    public int getThreadPoolMaxSize() {
        return threadPoolMaxSize;
    }

    public boolean isFilterCacheEnabled() {
        return filterCacheEnabled;
    }

    public int isFilterCacheMaxSize() {
        return filterCacheMaxSize;
    }
}
