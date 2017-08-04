/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 * Copyright (C) 2013-2014 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.backend;

import ca.polymtl.dorsal.libdelorean.interval.IStateInterval;
import ca.polymtl.dorsal.libdelorean.statevalue.IStateValue;
import org.eclipse.jdt.annotation.NonNull;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implement of a state history back-end to simply discards *all* the
 * intervals it receives. Obviously, no queries can be done on it. It is useful
 * for using with a StateSystem on which you will only want to do "ongoing"
 * requests.
 *
 * @author Alexandre Montplaisir
 */
class NullBackend implements IStateHistoryBackend {

    private final @NonNull String ssid;

    /**
     * Constructor
     *
     * @param ssid
     *            The state system's id
     */
    public NullBackend(@NonNull String ssid) {
        this.ssid = ssid;
    }

    @Override
    public String getSSID() {
        return ssid;
    }

    @Override
    public long getStartTime() {
        return 0;
    }

    @Override
    public long getEndTime() {
        return 0;
    }

    /**
     * The interval will be discarded when using a null backend.
     */
    @Override
    public void insertPastState(long stateStartTime, long stateEndTime,
            int quark, IStateValue value) {
        /* The interval is always discarded. */
    }

    @Override
    public void finishBuilding(long endTime) {
        /* Nothing to do */
    }

    @Override
    public FileInputStream supplyAttributeTreeReader() {
        return null;
    }

    @Override
    public File supplyAttributeTreeWriterFile() {
        return null;
    }

    @Override
    public long supplyAttributeTreeWriterFilePosition() {
        return -1;
    }

    @Override
    public void removeFiles() {
        /* Nothing to do */
    }

    @Override
    public void dispose() {
        /* Nothing to do */
    }

    /**
     * Null back-ends cannot run queries. Nothing will be put in
     * currentStateInfo.
     */
    @Override
    public void doQuery(List<IStateInterval> currentStateInfo, long t) {
        /* Cannot do past queries */
    }

    /**
     * Null back-ends cannot run queries. 'null' will be returned.
     *
     * @return Always returns null.
     */
    @Override
    public IStateInterval doSingularQuery(long t, int attributeQuark) {
        /* Cannot do past queries */
        return null;
    }

    // FIXME Needs to be implemented because of https://youtrack.jetbrains.com/issue/KT-4779
    @Override
    public void doPartialQuery(long t, @NotNull Set<Integer> quarks, @NotNull Map<Integer, IStateInterval> results) {
        quarks.forEach(quark -> {
            IStateInterval interval = doSingularQuery(t, quark);
            if (interval != null) {
                results.put(quark, interval);
            }
        });
    }
}
