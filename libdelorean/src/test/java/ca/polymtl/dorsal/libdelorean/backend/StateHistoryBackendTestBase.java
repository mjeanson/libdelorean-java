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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.eclipse.jdt.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

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
@RunWith(Parameterized.class)
public abstract class StateHistoryBackendTestBase {

    private static final long START_TIME = 0;
    private static final long END_TIME = 1000;

    /** The backend fixture to use in tests */
    protected IStateHistoryBackend fBackend;

    @Parameter(0)
    public String fName;
    @Parameter(1)
    public List<ITmfStateInterval> fIntervals;
    @Parameter(2)
    public int fNbAttributes;

    @Parameters(name = "Intervals={0}")
    public static Iterable<Object[]> parameters() {
        /**
         * Test the full query method by filling a small tree with intervals placed in a
         * "stair-like" fashion, like this:
         *
         * <pre>
         * |x----x----x---x|
         * |xx----x----x--x|
         * |x-x----x----x-x|
         * |x--x----x----xx|
         * |      ...      |
         * </pre>
         */
        List<ITmfStateInterval> cascadingIntervals = new ArrayList<>();
        int cacadingNbAttributes = 10;
        {
            long duration = 10;
            for (long t = START_TIME + 1; t <= END_TIME + duration; t++) {
                cascadingIntervals.add(new TmfStateInterval(
                        Math.max(START_TIME, t - duration),
                        Math.min(END_TIME, t - 1),
                        (int) t % cacadingNbAttributes,
                        TmfStateValue.newValueLong(t)));
            }
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
         */
        List<ITmfStateInterval> fullWidthIntervals = new ArrayList<>();
        int fullWidthNbdAttributes = 1000;
        {
            for (int attr = 0; attr < fullWidthNbdAttributes; attr++) {
                fullWidthIntervals.add(new TmfStateInterval(
                        START_TIME,
                        END_TIME,
                        attr,
                        TmfStateValue.newValueLong(attr)));
            }
        }

        List<ITmfStateInterval> oneInterval = Arrays.asList(new TmfStateInterval(START_TIME, END_TIME, 0, TmfStateValue.nullValue()));

        return Arrays.asList(
                new Object[] {"one-interval", oneInterval, 1 }, //$NON-NLS-1$
                new Object[] {"cascading", cascadingIntervals, cacadingNbAttributes }, //$NON-NLS-1$
                new Object[] {"full-width", fullWidthIntervals, fullWidthNbdAttributes } //$NON-NLS-1$
                );
    }

    /**
     * Test setup
     */
    @Before
    public void setup() {
        final IStateHistoryBackend backend = instantiateBackend(START_TIME);

        /* Insert the intervals into the backend */
        fIntervals.forEach(interval -> {
            backend.insertPastState(interval.getStartTime(),
                    interval.getEndTime(),
                    interval.getAttribute(),
                    interval.getStateValue());
        });
        backend.finishedBuilding(Math.max(END_TIME, backend.getEndTime()));

        fBackend = backend;

        afterInsertionCb();
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


    protected abstract IStateHistoryBackend instantiateBackend(long startTime);

    protected abstract void afterInsertionCb();

    /**
     * Test the full query method
     * {@link IStateHistoryBackend#doQuery(List, long)}, by filling a small tree
     * with the specified intervals and then querying at every single timestamp,
     * making sure all, and only, the expected intervals are returned.
     */
    @Test
    public void testFullQuery() {
        IStateHistoryBackend backend = fBackend;
        assertNotNull(backend);

        /*
         * Query at every valid time stamp, making sure only the expected intervals are
         * returned.
         */
        for (long t = backend.getStartTime(); t <= backend.getEndTime(); t++) {
            final long ts = t;

            List<@Nullable ITmfStateInterval> stateInfo = new ArrayList<>(fNbAttributes);
            IntStream.range(0, fNbAttributes).forEach(i -> stateInfo.add(null));
            backend.doQuery(stateInfo, t);

            stateInfo.forEach(interval -> {
                assertNotNull(interval);
                assertTrue(interval.intersects(ts));
            });
        }

        assertEquals(START_TIME, backend.getStartTime());
        assertEquals(END_TIME, backend.getEndTime());

    }

    /**
     * Test that the backend time is set correctly.
     */
    @Test
    public void testBackendEndTime() {
        long maxIntervalEndTime = fIntervals.stream()
                .mapToLong(ITmfStateInterval::getEndTime)
                .max().getAsLong();

        long expectedEndTime = Math.max(maxIntervalEndTime, END_TIME);
        assertEquals(expectedEndTime, fBackend.getEndTime());
    }
}
