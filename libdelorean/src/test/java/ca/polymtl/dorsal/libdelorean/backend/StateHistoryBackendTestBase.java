/*******************************************************************************
 * Copyright (c) 2016 Ericsson, EfficiOS Inc. and others
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package ca.polymtl.dorsal.libdelorean.backend;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.eclipse.jdt.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ca.polymtl.dorsal.libdelorean.exceptions.StateSystemDisposedException;
import ca.polymtl.dorsal.libdelorean.interval.ITmfStateInterval;
import ca.polymtl.dorsal.libdelorean.interval.TmfStateInterval;
import ca.polymtl.dorsal.libdelorean.statevalue.TmfStateValue;

/**
 * Abstract class to test implementations of the {@link IStateHistoryBackend}
 * interface.
 *
 * @author Patrick Tasse
 * @author Alexandre Montplaisir
 */
public abstract class StateHistoryBackendTestBase {

    /** The backend fixture to use in tests */
    protected IStateHistoryBackend fBackend;

    /**
     * Test setup
     */
    @Before
    public void setup() {
        fBackend = null;
    }

    /**
     * Test cleanup
     */
    @After
    public void teardown() {
        if (fBackend != null) {
            fBackend.dispose();
        }
    }

    /**
     * Prepares the 'fBackend' fixture to be used in tests.
     *
     * @param startTime
     *            The start time of the history
     * @param endTime
     *            The end time at which to close the history
     * @param intervals
     *            The intervals to insert in the history backend
     */
    protected abstract void prepareBackend(long startTime, long endTime,
            Collection<ITmfStateInterval> intervals);

    /**
     * Test the full query method
     * {@link IStateHistoryBackend#doQuery(List, long)}, by filling a small tree
     * with the specified intervals and then querying at every single timestamp,
     * making sure all, and only, the expected intervals are returned.
     *
     * @param startTime
     *            The start time of the history
     * @param endTime
     *            The end time of the history
     * @param nbAttr
     *            The number of attributes
     * @param intervals
     *            The list of intervals to insert
     */
    private void testDoQuery(long startTime, long endTime, int nbAttr,
            Collection<ITmfStateInterval> intervals) {

        prepareBackend(startTime, endTime, intervals);
        IStateHistoryBackend backend = fBackend;
        assertNotNull(backend);

        try {
            /*
             * Query at every valid time stamp, making sure only the expected
             * intervals are returned.
             */
            for (long t = backend.getStartTime(); t <= backend.getEndTime(); t++) {
                final long ts = t;

                List<@Nullable ITmfStateInterval> stateInfo = new ArrayList<>(nbAttr);
                IntStream.range(0, nbAttr).forEach(i -> stateInfo.add(null));
                backend.doQuery(stateInfo, t);

                stateInfo.forEach(interval -> {
                    assertNotNull(interval);
                    assertTrue(interval.intersects(ts));
                });
            }

            assertEquals(startTime, backend.getStartTime());
            assertEquals(endTime, backend.getEndTime());

        } catch (StateSystemDisposedException e) {
            fail(e.toString());
        }
    }

    /**
     * Test the full query method by filling a small tree with intervals placed
     * in a "stair-like" fashion, like this:
     *
     * <pre>
     * |x----x----x---x|
     * |xx----x----x--x|
     * |x-x----x----x-x|
     * |x--x----x----xx|
     * |      ...      |
     * </pre>
     *
     * and then querying at every single timestamp, making sure all, and only,
     * the expected intervals are returned.
     */
    @Test
    public void testCascadingIntervals() {
        final int nbAttr = 10;
        final long duration = 10;
        final long startTime = 0;
        final long endTime = 1000;

        List<ITmfStateInterval> intervals = new ArrayList<>();
        for (long t = startTime + 1; t <= endTime + duration; t++) {
            intervals.add(new TmfStateInterval(
                    Math.max(startTime, t - duration),
                    Math.min(endTime, t - 1),
                    (int) t % nbAttr,
                    TmfStateValue.newValueLong(t)));
        }

        testDoQuery(startTime, endTime, nbAttr, intervals);
    }

    /**
     * Test the full query method by filling a small backend with intervals that
     * take the full time range, like this:
     *
     * <pre>
     * |x-------------x|
     * |x-------------x|
     * |x-------------x|
     * |x-------------x|
     * |      ...      |
     * </pre>
     *
     * and then querying at every single timestamp, making sure all, and only,
     * the expected intervals are returned.
     */
    @Test
    public void testFullIntervals() {
        final int nbAttr = 1000;
        final long startTime = 0;
        final long endTime = 1000;

        List<ITmfStateInterval> intervals = new ArrayList<>();
        for (int attr = 0; attr < nbAttr; attr++) {
            intervals.add(new TmfStateInterval(
                    startTime,
                    endTime,
                    attr,
                    TmfStateValue.newValueLong(attr)));
        }

        testDoQuery(startTime, endTime, nbAttr, intervals);
    }

    /**
     * Test that the backend time is set correctly if there is only one interval
     * inserted, and both the interval time and backend end time are the same.
     */
    @Test
    public void testBackendEndTime() {
        long start = 0;
        long end = 100;

        ITmfStateInterval interval = new TmfStateInterval(start, end, 0, TmfStateValue.nullValue());

        testDoQuery(start, end, 1, Collections.singleton(interval));
    }
}
