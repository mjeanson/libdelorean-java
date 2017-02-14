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

import java.io.IOException;
import java.util.Collection;

import ca.polymtl.dorsal.libdelorean.backend.historytree.HistoryTreeBackend;
import ca.polymtl.dorsal.libdelorean.backend.historytree.ThreadedHistoryTreeBackend;
import ca.polymtl.dorsal.libdelorean.interval.ITmfStateInterval;

/**
 * Test the {@link ThreadedHistoryTreeBackend} class.
 *
 * @author Patrick Tasse
 * @author Alexandre Montplaisir
 */
public class ThreadedHistoryTreeBackendTest extends HistoryTreeBackendTest {

    private static final int QUEUE_SIZE = 10;

    /**
     * Constructor
     *
     * @param reOpen
     *            If we should use the backend as-is, or close it and re-open a
     *            new backend from the file.
     */
    public ThreadedHistoryTreeBackendTest(boolean reOpen) {
        super(reOpen);
    }

    @Override
    protected void prepareBackend(long startTime, long endTime, Collection<ITmfStateInterval> intervals) {
        try {
            final IStateHistoryBackend backend = new ThreadedHistoryTreeBackend(SSID, fTempFile, PROVIDER_VERSION,
                    startTime, QUEUE_SIZE, BLOCK_SIZE, MAX_CHILDREN);

            intervals.forEach(interval -> backend.insertPastState(interval.getStartTime(), interval.getEndTime(),
                    interval.getAttribute(), interval.getStateValue()));

            backend.finishedBuilding(Math.max(endTime, backend.getEndTime()));

            if (fReOpen) {
                /* Re-open the file using a standard history tree backend. */
                backend.dispose();
                fBackend = new HistoryTreeBackend(SSID, fTempFile, PROVIDER_VERSION);
            } else {
                fBackend = backend;
            }

        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

}
