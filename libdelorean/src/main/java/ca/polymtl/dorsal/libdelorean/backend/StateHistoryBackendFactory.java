/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 * Copyright (C) 2015 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.backend;

import java.io.File;
import java.io.IOException;

import org.eclipse.jdt.annotation.NonNullByDefault;

import ca.polymtl.dorsal.libdelorean.backend.historytree.HistoryTreeBackend;
import ca.polymtl.dorsal.libdelorean.backend.historytree.ThreadedHistoryTreeBackend;

/**
 * Factory for the various types {@link IStateHistoryBackend} supplied by this
 * plugin.
 *
 * @author Alexandre Montplaisir
 * @since 1.0
 */
@NonNullByDefault
public final class StateHistoryBackendFactory {

    private StateHistoryBackendFactory() {}

    /**
     * Create a new null-backend, which will not store any history intervals
     * (only the current state can be queried).
     *
     * @param ssid
     *            The ID for this state system
     * @return The state system backend
     */
    public static IStateHistoryBackend createNullBackend(String ssid) {
        return new NullBackend(ssid);
    }

    /**
     * Create a new in-memory backend. This backend will store all the history
     * intervals in memory, so it should not be used for anything serious.
     *
     * @param ssid
     *            The ID for this state system
     * @param startTime
     *            The start time of the state system and backend
     * @return The state system backend
     */
    public static IStateHistoryBackend createInMemoryBackend(String ssid, long startTime) {
        return new InMemoryBackend(ssid, startTime);
    }

    /**
     * Create a new backend using a History Tree. This backend stores all its
     * intervals on disk.
     *
     * By specifying a 'queueSize' parameter, the implementation that runs in a
     * separate thread can be used.
     *
     * @param ssid
     *            The state system's id
     * @param stateFile
     *            The filename/location where to store the state history (Should
     *            end in .ht)
     * @param providerVersion
     *            Version of of the state provider. We will only try to reopen
     *            existing files if this version matches the one in the
     *            framework.
     * @param startTime
     *            The earliest time stamp that will be stored in the history
     * @param queueSize
     *            The size of the interval insertion queue between the receiver
     *            and writer threads. 2000 - 10000 usually works well. If 0 is
     *            specified, no queue is used and the writes happen in the same
     *            thread.
     * @return The state system backend
     * @throws IOException
     *             Thrown if we can't create the file for some reason
     */
    public static IStateHistoryBackend createHistoryTreeBackendNewFile(String ssid,
            File stateFile, int providerVersion, long startTime, int queueSize) throws IOException {
        if (queueSize > 0) {
            return new ThreadedHistoryTreeBackend(ssid, stateFile, providerVersion, startTime, queueSize);
        }
        return new HistoryTreeBackend(ssid, stateFile, providerVersion, startTime);
    }

    /**
     * Create a new History Tree backend, but attempt to open an existing file
     * on disk. If the file cannot be found or recognized, an IOException will
     * be thrown.
     *
     * @param ssid
     *            The state system's id
     * @param stateFile
     *            Filename/location of the history we want to load
     * @param providerVersion
     *            Expected version of of the state provider plugin.
     * @return The state system backend
     * @throws IOException
     *             If we can't read the file, if it doesn't exist, is not
     *             recognized, or if the version of the file does not match the
     *             expected providerVersion.
     */
    public static IStateHistoryBackend createHistoryTreeBackendExistingFile(String ssid, File stateFile,
            int providerVersion) throws IOException {
        return new HistoryTreeBackend(ssid, stateFile, providerVersion);
    }
}
