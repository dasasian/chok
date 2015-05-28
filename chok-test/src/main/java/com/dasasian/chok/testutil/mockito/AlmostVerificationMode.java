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
package com.dasasian.chok.testutil.mockito;

import org.slf4j.Logger;
import org.mockito.exceptions.Reporter;
import org.mockito.internal.invocation.InvocationMatcher;
import org.mockito.internal.invocation.InvocationsFinder;
import org.mockito.internal.verification.api.VerificationData;
import org.mockito.internal.verification.checkers.AtLeastDiscrepancy;
import org.mockito.invocation.Invocation;
import org.mockito.invocation.Location;
import org.mockito.verification.VerificationMode;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Like {@link org.mockito.Mockito#times(int)} but with allowing a aberration.
 */
public class AlmostVerificationMode implements VerificationMode {

    private static final Logger LOG = LoggerFactory.getLogger(AlmostVerificationMode.class);

    private final int _wantedNumberOfInvocations;
    private final int _aberration;

    public AlmostVerificationMode(int wantedNumberOfInvocations, int aberration) {
        _wantedNumberOfInvocations = wantedNumberOfInvocations;
        _aberration = aberration;
    }

    @Override
    public void verify(VerificationData data) {
        List<Invocation> invocations = data.getAllInvocations();
        InvocationMatcher wanted = data.getWanted();

        Reporter reporter = new Reporter();
        InvocationsFinder finder = new InvocationsFinder();
        List<Invocation> found = finder.findInvocations(invocations, wanted);
        int invocationCount = found.size();
        if (invocationCount != _wantedNumberOfInvocations) {
            LOG.warn("invocation count is " + invocationCount + " expected was " + _wantedNumberOfInvocations + " +-" + _aberration);
        }

        int minNumberOfInvocations = _wantedNumberOfInvocations - _aberration;
        if (invocationCount < minNumberOfInvocations) {
            Location lastLocation = finder.getLastLocation(invocations);
            reporter.tooLittleActualInvocations(new AtLeastDiscrepancy(minNumberOfInvocations, invocationCount), wanted, lastLocation);
        }
        int maxNumberOfInvocations = _wantedNumberOfInvocations + _aberration;
        if (invocationCount > maxNumberOfInvocations) {
            reporter.wantedAtMostX(maxNumberOfInvocations, invocationCount);
        }
    }

}
