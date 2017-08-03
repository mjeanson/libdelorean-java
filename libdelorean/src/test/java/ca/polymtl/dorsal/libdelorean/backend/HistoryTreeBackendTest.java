/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 * Copyright (C) 2016 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.backend;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.eclipse.jdt.annotation.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ca.polymtl.dorsal.libdelorean.backend.historytree.HistoryTreeBackend;

/**
 * Test the {@link HistoryTreeBackend} class.
 *
 * @author Patrick Tasse
 * @author Alexandre Montplaisir
 */
@RunWith(Parameterized.class)
public class HistoryTreeBackendTest extends StateHistoryBackendTestBase {

    /** State system ID */
    protected static final @NonNull String SSID = "test"; //$NON-NLS-1$
    /** Provider version */
    protected static final int PROVIDER_VERSION = 0;

    /** Default maximum number of children nodes */
    private static final int MAX_CHILDREN = 2;
    /** Default block size */
    private static final int BLOCK_SIZE = 4096;

    /** History tree file */
    protected File fTempFile;

    @Override
    @Before
    public void setup() {
        try {
            fTempFile = File.createTempFile(getClass().getSimpleName(), ".ht"); //$NON-NLS-1$
        } catch (IOException e) {
            fail(e.getMessage());
        }
        super.setup();
    }

    @Override
    @After
    public void teardown() {
        /*
         * We need the super-class's teardown() to happen first, so we override
         * this method to do so, because the default order would run it *after*
         * the file.delete() here.
         */
        super.teardown();
        if (fTempFile != null) {
            fTempFile.delete();
        }
    }

    @Override
    protected IStateHistoryBackend instantiateBackend(long startTime) {
        return new HistoryTreeBackend(SSID, fTempFile, PROVIDER_VERSION, startTime, BLOCK_SIZE, MAX_CHILDREN);
    }

    @Override
    protected void afterInsertionCb() {}
}
