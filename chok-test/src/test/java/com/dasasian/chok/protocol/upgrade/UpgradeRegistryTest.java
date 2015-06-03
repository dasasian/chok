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
package com.dasasian.chok.protocol.upgrade;

import com.dasasian.chok.protocol.InteractionProtocol;
import com.dasasian.chok.protocol.metadata.Version;
import com.dasasian.chok.testutil.AbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class UpgradeRegistryTest extends AbstractTest {

    private UpgradeAction _upgradeAction = Mockito.mock(UpgradeAction.class);
    private InteractionProtocol _protocol = Mockito.mock(InteractionProtocol.class);

    @Test
    public void testNoUpgradeFound() throws Exception {
        Mockito.when(_protocol.getVersion()).thenReturn(createVersion("0.1"));
        Version distributionVersion = createVersion("0.2");
        assertNull(UpgradeRegistry.findUpgradeAction(_protocol, distributionVersion));
    }

    @Test
    public void testNoUpgradeNeeded() throws Exception {
        UpgradeRegistry.registerUpgradeAction("0.3", "0.4", _upgradeAction);
        Mockito.when(_protocol.getVersion()).thenReturn(createVersion("0.4"));
        Version distributionVersion = createVersion("0.4");
        assertNull(UpgradeRegistry.findUpgradeAction(_protocol, distributionVersion));
    }

    @Test
    public void testUpgrade() throws Exception {
        UpgradeRegistry.registerUpgradeAction("0.3", "0.4", _upgradeAction);

        Mockito.when(_protocol.getVersion()).thenReturn(createVersion("0.3"));
        Version distributionVersion = createVersion("0.4");
        assertNotNull(UpgradeRegistry.findUpgradeAction(_protocol, distributionVersion));
    }

    @Test
    public void testUpgradeWithDevVersion() throws Exception {
        UpgradeRegistry.registerUpgradeAction("0.3", "0.4", _upgradeAction);

        Mockito.when(_protocol.getVersion()).thenReturn(createVersion("0.3"));
        Version distributionVersion = createVersion("0.4-dev");
        assertNotNull(UpgradeRegistry.findUpgradeAction(_protocol, distributionVersion));
    }

    private Version createVersion(String number) {
        return new Version(number, "-", "-", "-");
    }
}
