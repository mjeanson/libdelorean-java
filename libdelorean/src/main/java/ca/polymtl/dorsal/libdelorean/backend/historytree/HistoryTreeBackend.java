/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 * Copyright (C) 2012-2015 Ericsson
 * Copyright (C) 2010-2011 École Polytechnique de Montréal
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.backend.historytree;

import ca.polymtl.dorsal.libdelorean.backend.IStateHistoryBackend;
import ca.polymtl.dorsal.libdelorean.exceptions.StateSystemDisposedException;
import ca.polymtl.dorsal.libdelorean.exceptions.TimeRangeException;
import ca.polymtl.dorsal.libdelorean.interval.IStateInterval;
import ca.polymtl.dorsal.libdelorean.statevalue.IStateValue;
import ca.polymtl.dorsal.libdelorean.statevalue.StateValue;
import org.eclipse.jdt.annotation.NonNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * History Tree backend for storing a state history. This is the basic version
 * that runs in the same thread as the class creating it.
 *
 * @author Alexandre Montplaisir
 */
public class HistoryTreeBackend implements IStateHistoryBackend {

    private static final Logger LOGGER = Logger.getLogger(HistoryTreeBackend.class.getName());

    private final @NonNull String fSsid;

    /**
     * The history tree that sits underneath.
     */
    private final HistoryTree fSht;

    /** Indicates if the history tree construction is done */
    private volatile boolean fFinishedBuilding = false;

    /**
     * Indicates if the history tree construction is done
     *
     * @return if the history tree construction is done
     */
    protected boolean isFinishedBuilding() {
        return fFinishedBuilding;
    }

    /**
     * Sets if the history tree is finished building
     *
     * @param isFinishedBuilding
     *            is the history tree finished building
     */
    protected void setFinishedBuilding(boolean isFinishedBuilding) {
        fFinishedBuilding = isFinishedBuilding;
    }

    /**
     * Constructor for new history files. Use this when creating a new history
     * from scratch.
     *
     * @param ssid
     *            The state system's ID
     * @param newStateFile
     *            The filename/location where to store the state history (Should
     *            end in .ht)
     * @param providerVersion
     *            Version of of the state provider. We will only try to reopen
     *            existing files if this version matches the one in the
     *            framework.
     * @param startTime
     *            The earliest time stamp that will be stored in the history
     * @param blockSize
     *            The size of the blocks in the history file. This should be a
     *            multiple of 4096.
     * @param maxChildren
     *            The maximum number of children each core node can have
     * @throws IOException
     *             Thrown if we can't create the file for some reason
     */
    public HistoryTreeBackend(@NonNull String ssid,
            File newStateFile,
            int providerVersion,
            long startTime,
            int blockSize,
            int maxChildren) throws IOException {
        fSsid = ssid;
        fSht = new HistoryTree(newStateFile, blockSize, maxChildren,
                providerVersion, startTime);
    }

    /**
     * Constructor for new history files. Use this when creating a new history
     * from scratch. This version supplies sane defaults for the configuration
     * parameters.
     *
     * @param ssid
     *            The state system's id
     * @param newStateFile
     *            The filename/location where to store the state history (Should
     *            end in .ht)
     * @param providerVersion
     *            Version of of the state provider. We will only try to reopen
     *            existing files if this version matches the one in the
     *            framework.
     * @param startTime
     *            The earliest time stamp that will be stored in the history
     * @throws IOException
     *             Thrown if we can't create the file for some reason
     */
    public HistoryTreeBackend(@NonNull String ssid, File newStateFile, int providerVersion, long startTime)
            throws IOException {
        this(ssid, newStateFile, providerVersion, startTime, 64 * 1024, 50);
    }

    /**
     * Existing history constructor. Use this to open an existing state-file.
     *
     * @param ssid
     *            The state system's id
     * @param existingStateFile
     *            Filename/location of the history we want to load
     * @param providerVersion
     *            Expected version of of the state provider plugin.
     * @throws IOException
     *             If we can't read the file, if it doesn't exist, is not
     *             recognized, or if the version of the file does not match the
     *             expected providerVersion.
     */
    public HistoryTreeBackend(@NonNull String ssid, File existingStateFile, int providerVersion)
            throws IOException {
        fSsid = ssid;
        fSht = new HistoryTree(existingStateFile, providerVersion);
        fFinishedBuilding = true;
    }

    /**
     * Get the History Tree built by this backend.
     *
     * @return The history tree
     */
    protected HistoryTree getSHT() {
        return fSht;
    }

    @Override
    public String getSSID() {
        return fSsid;
    }

    @Override
    public long getStartTime() {
        return fSht.getTreeStart();
    }

    @Override
    public long getEndTime() {
        return fSht.getTreeEnd();
    }

    @Override
    public void insertPastState(long stateStartTime, long stateEndTime,
            int quark, IStateValue value) throws TimeRangeException {
        HTInterval interval = new HTInterval(stateStartTime, stateEndTime,
                quark, (StateValue) value);

        /* Start insertions at the "latest leaf" */
        fSht.insertInterval(interval);
    }

    @Override
    public void finishedBuilding(long endTime) {
        fSht.closeTree(endTime);
        fFinishedBuilding = true;
    }

    @Override
    public FileInputStream supplyAttributeTreeReader() {
        return fSht.supplyATReader();
    }

    @Override
    public File supplyAttributeTreeWriterFile() {
        return fSht.supplyATWriterFile();
    }

    @Override
    public long supplyAttributeTreeWriterFilePosition() {
        return fSht.supplyATWriterFilePos();
    }

    @Override
    public void removeFiles() {
        fSht.deleteFile();
    }

    @Override
    public void dispose() {
        if (fFinishedBuilding) {
            fSht.closeFile();
        } else {
            /*
             * The build is being interrupted, delete the file we partially
             * built since it won't be complete, so shouldn't be re-used in the
             * future (.deleteFile() will close the file first)
             */
            fSht.deleteFile();
        }
    }

    @Override
    public void doQuery(List<IStateInterval> stateInfo, long t)
            throws TimeRangeException, StateSystemDisposedException {
        checkValidTime(t);

        /* We start by reading the information in the root node */
        HistoryTreeNode currentNode = fSht.getRootNode();
        currentNode.writeInfoFromNode(stateInfo, t);

        /* Then we follow the branch down in the relevant children */
        try {
            while (currentNode instanceof CoreNode) {
                currentNode = fSht.selectNextChild((CoreNode) currentNode, t);
                currentNode.writeInfoFromNode(stateInfo, t);
            }
        } catch (ClosedChannelException e) {
            throw new StateSystemDisposedException(e);
        }

        /*
         * The stateInfo should now be filled with everything needed, we pass
         * the control back to the State System.
         */
    }

    @Override
    public IStateInterval doSingularQuery(long t, int attributeQuark)
            throws TimeRangeException, StateSystemDisposedException {
        return getRelevantInterval(t, attributeQuark);
    }

    @Override
    public void doPartialQuery(long t, Set<Integer> quarks, Map<Integer, IStateInterval> results) {
        checkValidTime(t);

        int remaining = quarks.size();

        /* We start by reading the information in the root node. */
        HistoryTreeNode currentNode = fSht.getRootNode();
        currentNode.takeReadLock();
        try {
            for (IStateInterval interval : currentNode.getIntervals()) {
                if (interval == null) {
                    continue;
                }
                int quark = interval.getAttribute();
                if (quarks.contains(quark) && interval.intersects(t)) {
                    results.put(quark, interval);
                    remaining--;
                }
            }
        } finally {
            currentNode.releaseReadLock();
        }

        /* Then we follow the branch down in the relevant children. */
        try {
            while (remaining > 0 && currentNode instanceof CoreNode) {
                currentNode = fSht.selectNextChild((CoreNode) currentNode, t);
                currentNode.takeReadLock();
                try {
                    for (IStateInterval interval : currentNode.getIntervals()) {
                        if (interval == null) {
                            continue;
                        }
                        int quark = interval.getAttribute();
                        if (quarks.contains(quark) && interval.intersects(t)) {
                            results.put(quark, interval);
                            remaining--;
                        }
                    }
                } finally {
                    currentNode.releaseReadLock();
                }
            }
        } catch (ClosedChannelException e) {
            throw new StateSystemDisposedException(e);
        }
    }

    private void checkValidTime(long t) {
        long startTime = getStartTime();
        long endTime = getEndTime();
        if (t < startTime || t > endTime) {
            throw new TimeRangeException(String.format("%s Time:%d, Start:%d, End:%d", //$NON-NLS-1$
                    fSsid, t, startTime, endTime));
        }
    }

    /**
     * Inner method to find the interval in the tree containing the requested
     * key/timestamp pair, wherever in which node it is.
     *
     * @param t
     * @param key
     * @return The node containing the information we want
     */
    private HTInterval getRelevantInterval(long t, int key)
            throws TimeRangeException, StateSystemDisposedException {
        checkValidTime(t);

        HistoryTreeNode currentNode = fSht.getRootNode();
        HTInterval interval = currentNode.getRelevantInterval(key, t);

        try {
            while (interval == null && currentNode instanceof CoreNode) {
                currentNode = fSht.selectNextChild((CoreNode) currentNode, t);
                interval = currentNode.getRelevantInterval(key, t);
            }
        } catch (ClosedChannelException e) {
            throw new StateSystemDisposedException(e);
        }
        return interval;
    }

    /**
     * Return the size of the tree history file
     *
     * @return The current size of the history file in bytes
     */
    public long getFileSize() {
        return fSht.getFileSize();
    }

}
