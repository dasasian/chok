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
package com.dasasian.chok.util;

import com.dasasian.chok.master.DefaultDistributionPolicy;
import com.dasasian.chok.master.IDeployPolicy;

/**
 * User: damith.chandrasekara
 * Date: 7/4/13
 */
public class MasterConfigurationLoader {
    private final static String DEPLOY_POLICY = "master.deploy.policy";
    private final static String SAFE_MODE_MAX_TIME = "safemode.maxTime";

    public static MasterConfiguration loadConfiguration() throws InstantiationException, IllegalAccessException {
        return loadConfiguration(null, null);
    }

    public static MasterConfiguration loadConfiguration(Class<? extends IDeployPolicy> overrideDeployPolicyClass, Integer overrideSafeModeMaxTime) throws IllegalAccessException, InstantiationException {
        ChokConfiguration chokConfiguration = new ChokConfiguration("/chok.master.properties");

        Class<? extends IDeployPolicy> deployPolicyClass = overrideDeployPolicyClass != null ? overrideDeployPolicyClass : ClassUtil.forName(chokConfiguration.getProperty(DEPLOY_POLICY, DefaultDistributionPolicy.class.getName()), IDeployPolicy.class);

        int safeModeMaxTime = overrideSafeModeMaxTime != null ? overrideSafeModeMaxTime : chokConfiguration.getInt(SAFE_MODE_MAX_TIME, 25);

        IDeployPolicy deployPolicy = deployPolicyClass.newInstance();

        return new MasterConfiguration(deployPolicy, safeModeMaxTime);
    }
}
