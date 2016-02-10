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
package com.dasasian.chok.master;

import com.google.common.collect.ImmutableSetMultimap;

import java.util.Collection;
import java.util.Set;

/**
 * Exchangeable policy for which creates an distribution plan for the shards of
 * one index.
 */
public interface IDeployPolicy {

    /**
     * Creates a distribution plan for the shards of one index. Note that the
     * index can already be deployed, in that case its more a "replication" plan.
     *
     *
     * @param shards the shards that are being deployed
     * @param nodes the nodes that are alive
     *@param currentShard2NodesMap all current deployments of the shards of the one index to
     *                              distribute/replicate
     * @param replicationLevel number of replicas
     * @return the plan of node to shard mappings
     */
    ImmutableSetMultimap<String, String> createDistributionPlan(final Set<String> shards, final Collection<String> nodes,
                                                                final ImmutableSetMultimap<String, String> currentShard2NodesMap,
                                                                final int replicationLevel);

}
