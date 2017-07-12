/*******************************************************************************
 * Copyright (c) 2016 EfficiOS Inc., Alexandre Montplaisir
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package ca.polymtl.dorsal.libdelorean.statedump;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.eclipse.jdt.annotation.NonNull;
import org.junit.Test;

import com.google.common.io.MoreFiles;

import ca.polymtl.dorsal.libdelorean.statevalue.ITmfStateValue;
import ca.polymtl.dorsal.libdelorean.statevalue.TmfStateValue;

/**
 * Tests for {@link Statedump}.
 *
 * @author Alexandre Montplaisir
 */
public class StatedumpTest {

    /**
     * Test that a manually-written statedump is serialized then read correctly.
     */
    @Test
    public void testSimpleStatedump() {
        Path dir = null;
        try {
            dir = requireNonNull(Files.createTempDirectory("ss-serialization-test"));
            String ssid = "test-ssid";
            final int nbAttributes = 6;
            final int version = 0;

            List<String @NonNull []> initialAttributes = Arrays.asList(
                    new String[] { "Threads" },
                    new String[] { "Threads", "1000" },
                    new String[] { "Threads", "1000", "Status" },
                    new String[] { "Threads", "2000" },
                    new String[] { "Threads", "2000", "Status" },
                    new String[] { "Threads", "2000", "PPID" });

            List<@NonNull ITmfStateValue> initialValues = Arrays.asList(
                    TmfStateValue.nullValue(),
                    TmfStateValue.nullValue(),
                    TmfStateValue.newValueString("Running"),
                    TmfStateValue.nullValue(),
                    TmfStateValue.newValueInt(1),
                    TmfStateValue.newValueLong(1000L));

            Statedump statedump = new Statedump(initialAttributes, initialValues, version);
            statedump.dumpState(dir, ssid);

            Statedump results = Statedump.loadState(dir, ssid);
            assertNotNull(results);

            assertEquals(version, results.getVersion());

            List<String[]> newAttributes = results.getAttributes();
            List<ITmfStateValue> newValues = results.getStates();
            assertEquals(nbAttributes, newAttributes.size());
            assertEquals(nbAttributes, newValues.size());

            for (int i = 0; i < nbAttributes; i++) {
                assertArrayEquals(initialAttributes.get(i), newAttributes.get(i));
                assertEquals(initialValues.get(i), newValues.get(i));
            }

        } catch (IOException e) {
            fail(e.getMessage());

        } finally {
            if (dir != null) {
                try {
                    MoreFiles.deleteRecursively(dir);
                } catch (IOException e) {
                }
            }
        }
    }
}
