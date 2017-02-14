/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 * Copyright (C) 2013-2014 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.interval;

import java.util.Comparator;

/**
 * Comparator for ITmfStateInterval, using their *end times*. Making intervals
 * Comparable wouldn't be clear if it's using their start or end times (or maybe
 * even values), so separate comparators are provided.
 *
 * @author Alexandre Montplaisir
 */
class TmfIntervalEndComparator implements Comparator<ITmfStateInterval> {

    @Override
    public int compare(ITmfStateInterval o1, ITmfStateInterval o2) {
        long e1 = o1.getEndTime();
        long e2 = o2.getEndTime();

        if (e1 < e2) {
            return -1;
        } else if (e1 > e2) {
            return 1;
        } else {
            return 0;
        }
    }

}
