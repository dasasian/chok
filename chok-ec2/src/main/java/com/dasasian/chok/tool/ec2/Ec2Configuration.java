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
package com.dasasian.chok.tool.ec2;

import com.dasasian.chok.util.ChokConfiguration;

@SuppressWarnings("serial")
public class Ec2Configuration extends ChokConfiguration {

    final static String ACCOUNT_ID = "aws.accountId";
    final static String ACCESS_KEY = "aws.accessKeyId";
    final static String SECRET_ACCESS_KEY = "aws.secretAccessKey";
    final static String KEY_PAIR_NAME = "aws.keyPairName";
    private static final String AIM = "aws.aim";

    public Ec2Configuration() {
        super("/chok.ec2.properties");
    }

    public String getAccountId() {
        return getProperty(ACCOUNT_ID);
    }

    public String getAccessKey() {
        return getProperty(ACCESS_KEY);
    }

    public String getSecretAccessKey() {
        return getProperty(SECRET_ACCESS_KEY);
    }

    public String getKeyName() {
        return getProperty(KEY_PAIR_NAME);
    }

    public String getKeyPath() {
        return getProperty("aws.keyPath");
    }

    public String getAim() {
        return getProperty(AIM);
    }

}
