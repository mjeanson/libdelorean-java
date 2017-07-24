/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 * Copyright (C) 2012-2014 Ericsson
 * Copyright (C) 2010-2011 École Polytechnique de Montréal
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.interval;

import org.eclipse.jdt.annotation.NonNull;

import com.google.common.base.MoreObjects;

import ca.polymtl.dorsal.libdelorean.statevalue.ITmfStateValue;

import java.util.Objects;

/**
 * The StateInterval represents the "state" a particular attribute was in, at a
 * given time. It is the main object being returned from queries to the state
 * system.
 *
 * @author Alexandre Montplaisir
 */
public final class TmfStateInterval implements ITmfStateInterval {

    private final long start;
    private final long end;
    private final int attribute;
    private final @NonNull ITmfStateValue sv;

    /**
     * Construct an interval from its given parameters
     *
     * @param start
     *            Start time
     * @param end
     *            End time
     * @param attribute
     *            Attribute linked to this interval
     * @param sv
     *            State value this interval will contain
     */
    public TmfStateInterval(long start, long end, int attribute,
            @NonNull ITmfStateValue sv) {
        this.start = start;
        this.end = end;
        this.attribute = attribute;
        this.sv = sv;
    }

    @Override
    public long getStartTime() {
        return start;
    }

    @Override
    public long getEndTime() {
        return end;
    }

    @Override
    public int getAttribute() {
        return attribute;
    }

    @Override
    public ITmfStateValue getStateValue() {
        return sv;
    }

    @Override
    public boolean intersects(long timestamp) {
        if (start <= timestamp) {
            if (end >= timestamp) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, attribute, sv);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TmfStateInterval that = (TmfStateInterval) o;
        return (start == that.start
                && end == that.end
                && attribute == that.attribute
                && Objects.equals(sv, that.sv));
    }

    @Override
    public String toString() {
        /* Only used for debugging */
        return MoreObjects.toStringHelper(this)
            .add("start", start) //$NON-NLS-1$
            .add("end", end) //$NON-NLS-1$
            .add("key", attribute) //$NON-NLS-1$
            .add("value", sv.toString()) //$NON-NLS-1$
            .toString();
    }

}
