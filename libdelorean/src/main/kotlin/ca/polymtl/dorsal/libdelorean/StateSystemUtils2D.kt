/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean

import ca.polymtl.dorsal.libdelorean.interval.IStateInterval
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
fun IStateSystemReader.iterator2D(rangeStart: Long, rangeEnd: Long, resolution: Long, quarks: Set<Int>): Iterator<IStateInterval> {
    
    /* Check parameters */
    if (rangeStart < this.startTime || rangeEnd > this.currentEndTime) {
        throw IllegalArgumentException()
    }
    if (quarks.isEmpty()) {
        return Collections.emptyIterator()
    }

    val initialTargets = quarks.map { Pair(it, rangeStart) }
    return StateIterator(this, rangeStart, rangeEnd, resolution, initialTargets)
}

@VisibleForTesting
internal class StateIterator(private val ss: IStateSystemReader,
                             private val rangeStart: Long,
                             private val rangeEnd: Long,
                             private val resolution: Long,
                             initialTargets: Collection<Pair<Int, Long>>): AbstractIterator<IStateInterval>() {

    /* Prio queue of Pair<Interval, nextResolutionPoint> */
    private val prio = PriorityQueue<QueryTarget>(initialTargets.size, compareBy { it.ts } )
    init {
        /* Populate the queue from the initial intervals */
        prio.addAll(initialTargets.map { QueryTarget(it.first, it.second) })
    }

    override fun computeNext() {
        var target = prio.poll() ?: return done()
        var interval = ss.querySingleState(target.ts, target.quark)
        var nextResolutionPoint = target.ts + resolution
        while (!(interval.intersects(target.ts) && interval.intersects(nextResolutionPoint))) {
            if (nextResolutionPoint <= rangeEnd) {
                prio.offer(QueryTarget(target.quark, nextResolutionPoint))
            }
            target = prio.poll() ?: return done()
            interval = ss.querySingleState(target.ts, target.quark)
            nextResolutionPoint = target.ts + resolution
        }
        /* "interval" is now one we will want the iteration to return */
        setNext(interval)
        nextResolutionPoint = determineNextQueryTs(interval, rangeStart, target.ts, resolution)
        if (nextResolutionPoint <= rangeEnd) {
            prio.offer(QueryTarget(target.quark, nextResolutionPoint))
        }
    }

    inner class QueryTarget(val quark: Int, val ts: Long) {
        init {
            /* "ts" should always be a multiple of the resolution */
            if ((ts - rangeStart) % resolution != 0L) {
                throw IllegalArgumentException()
            }
        }
    }

}

@VisibleForTesting
internal fun determineNextQueryTs(interval: IStateInterval,
                                  rangeStart: Long,
                                  currentQueryTs: Long,
                                  resolution: Long): Long {

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
