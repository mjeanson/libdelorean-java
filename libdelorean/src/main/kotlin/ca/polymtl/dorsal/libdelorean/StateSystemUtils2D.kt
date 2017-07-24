/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean

import ca.polymtl.dorsal.libdelorean.interval.ITmfStateInterval
import com.google.common.annotations.VisibleForTesting
import java.util.*

/**
 * Query the state system in a two-dimensional fashion in one go, one
 * dimension being time and the other being a set of quarks.
 *
 * It will return a lazy Iterable returning the matching state
 * intervals. These interval have no ordering guarantees according to
 * their quarks, but should be sorted time-wise.
 */
fun ITmfStateSystem.iterator2D(rangeStart: Long, rangeEnd: Long, resolution: Long, quarks: Set<Int>): Iterator<ITmfStateInterval> {
    
    /* Check parameters */
    if (rangeStart < this.startTime || rangeEnd > this.currentEndTime) {
        throw IllegalArgumentException()
    }

    /* Start with a partial query of the requested quarks */
    val initialStates = queryStates(rangeStart, quarks)
    if (initialStates.values.isEmpty()) {
        return Collections.emptyIterator()
    }

    return StateIterator(this, rangeStart, rangeEnd, resolution, initialStates.values)
}

@VisibleForTesting
internal class StateIterator(private val ss: ITmfStateSystem,
                            private val rangeStart: Long,
                            private val rangeEnd: Long,
                            private val resolution: Long,
                            initialIntervals: Collection<ITmfStateInterval>): AbstractIterator<ITmfStateInterval>() {

    companion object {

        @VisibleForTesting
        internal fun determineNextQueryTs(interval: ITmfStateInterval, rangeStart: Long,
                                          currentQueryTs: Long, resolution: Long): Long {
            if (!interval.intersects(currentQueryTs)) {
                /* Logic error! */
                throw IllegalStateException()
            }

            val nextResolutionPoint = currentQueryTs + resolution
            return if (nextResolutionPoint > interval.endTime) {
                nextResolutionPoint
            } else {
                val base = interval.endTime - rangeStart + 1
                val newBase = roundToClosestHigherMultiple(base, resolution)
                newBase + rangeStart
            }
        }

        /**
         * Find the multiple of 'multipleOf' that is greater but closest to
         * 'number'. If 'number' is already a multiple of 'multipleOf', the same
         * value will be returned.
         *
         * @param number
         *            The starting number
         * @param multipleOf
         *            We want the returned value to be a multiple of this number
         * @return The closest, greater multiple
         */
        private fun roundToClosestHigherMultiple(number: Long, multipleOf: Long): Long {
            return (Math.ceil(number.toDouble() / multipleOf) * multipleOf).toLong()
        }
    }

    /* Prio queue of Pair<Interval, nextQueryTimestamp> */
    private val prio = PriorityQueue<Pair<ITmfStateInterval, Long>>(initialIntervals.size, compareBy { it.first.endTime } )
    init {
        /* Populate the queue from the initial intervals */
        prio.addAll(initialIntervals.map { interval -> Pair(interval, determineNextQueryTs(interval, rangeStart, rangeStart, resolution)) })
    }

    override fun computeNext() {
        val currentIntervalPair = prio.poll() ?: return done()

        /*
         * Fetch the replacement interval (if there is one) and place
         * it into the queue.
         */
        val quark = currentIntervalPair.first.attribute
        val queryTs = currentIntervalPair.second
        if (queryTs <= rangeEnd) {
            val replacementInterval = ss.querySingleState(queryTs, quark)
            val nextQueryTs = determineNextQueryTs(replacementInterval, rangeStart, queryTs, resolution)
            prio.offer(Pair(replacementInterval, nextQueryTs))
        }

        return setNext(currentIntervalPair.first)
    }

}

