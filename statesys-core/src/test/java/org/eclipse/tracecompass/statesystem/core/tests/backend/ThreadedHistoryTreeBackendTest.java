/*******************************************************************************
 * Copyright (c) 2016 Ericsson, EfficiOS Inc. and others
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package org.eclipse.tracecompass.statesystem.core.tests.backend;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.eclipse.tracecompass.internal.statesystem.core.backend.historytree.HistoryTreeBackend;
import org.eclipse.tracecompass.internal.statesystem.core.backend.historytree.ThreadedHistoryTreeBackend;
import org.eclipse.tracecompass.statesystem.core.backend.IStateHistoryBackend;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;

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
    public ThreadedHistoryTreeBackendTest(Boolean reOpen) {
        super(reOpen);
    }

    @Override
    protected void prepareBackend(long startTime, long endTime,
            List<ITmfStateInterval> intervals) {
        try {
            IStateHistoryBackend backend = new ThreadedHistoryTreeBackend(SSID, fTempFile,
                    PROVIDER_VERSION, startTime, QUEUE_SIZE, BLOCK_SIZE, MAX_CHILDREN);
            for (ITmfStateInterval interval : intervals) {
                backend.insertPastState(interval.getStartTime(), interval.getEndTime(),
                        interval.getAttribute(), interval.getStateValue());
            }
            backend.finishedBuilding(Math.max(endTime, backend.getEndTime()));

            if (fReOpen) {
                /* Re-open the file using a standard history tree backend. */
                backend.dispose();
                backend = new HistoryTreeBackend(SSID, fTempFile, PROVIDER_VERSION);
            }
            fBackend = backend;

        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

}
