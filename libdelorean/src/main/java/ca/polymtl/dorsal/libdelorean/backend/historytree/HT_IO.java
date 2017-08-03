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

package ca.polymtl.dorsal.libdelorean.backend.historytree;

import org.eclipse.jdt.annotation.NonNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class abstracts inputs/outputs of the HistoryTree nodes.
 *
 * It contains all the methods and descriptors to handle reading/writing nodes
 * to the tree-file on disk and all the caching mechanisms.
 *
 * This abstraction is mainly for code isolation/clarification purposes. Every
 * HistoryTree must contain 1 and only 1 HT_IO element.
 *
 * @author Alexandre Montplaisir
 */
class HT_IO {

    private static final Logger LOGGER = Logger.getLogger(HT_IO.class.getName());

    private final File fStateFile;
    private final int fBlockSize;
    private final int fMaxChildren;

    /* Fields related to the file I/O */
    private final FileInputStream fFileInputStream;
    private final FileOutputStream fFileOutputStream;
    private final FileChannel fFileChannelIn;
    private final FileChannel fFileChannelOut;

    // TODO test/benchmark optimal cache size
    /**
     * Cache size, must be a power of 2
     */
    private static final int CACHE_SIZE = 256;
    private static final int CACHE_MASK = CACHE_SIZE - 1;
    private final HistoryTreeNode fNodeCache[] = new HistoryTreeNode[CACHE_SIZE];

    /**
     * Standard constructor
     *
     * @param stateFile
     *            The name of the history file
     * @param blockSize
     *            The size of each "block" on disk. One node will always fit in
     *            one block.
     * @param maxChildren
     *            The maximum number of children allowed per core (non-leaf)
     *            node.
     * @param newFile
     *            Flag indicating that the file must be created from scratch
     * @throws IOException
     *             An exception can be thrown when file cannot be accessed
     */
    public HT_IO(File stateFile, int blockSize, int maxChildren, boolean newFile) throws IOException {
        fStateFile = stateFile;
        fBlockSize = blockSize;
        fMaxChildren = maxChildren;

        File historyTreeFile = fStateFile;
        if (newFile) {
            boolean success1 = true;
            /* Create a new empty History Tree file */
            if (historyTreeFile.exists()) {
                success1 = historyTreeFile.delete();
            }
            boolean success2 = historyTreeFile.createNewFile();
            if (!(success1 && success2)) {
                /* It seems we do not have permission to create the new file */
                throw new IOException("Cannot create new file at " + //$NON-NLS-1$
                        historyTreeFile.getName());
            }
            fFileInputStream = new FileInputStream(historyTreeFile);
            fFileOutputStream = new FileOutputStream(historyTreeFile, false);
        } else {
            /*
             * We want to open an existing file, make sure we don't squash the
             * existing content when opening the fos!
             */
            fFileInputStream = new FileInputStream(historyTreeFile);
            fFileOutputStream = new FileOutputStream(historyTreeFile, true);
        }
        fFileChannelIn = fFileInputStream.getChannel();
        fFileChannelOut = fFileOutputStream.getChannel();
    }

    /**
     * Read a node from the file on disk.
     *
     * @param seqNumber
     *            The sequence number of the node to read.
     * @return The object representing the node
     * @throws ClosedChannelException
     *             Usually happens because the file was closed while we were
     *             reading. Instead of using a big reader-writer lock, we'll
     *             just catch this exception.
     */
    public synchronized @NonNull HistoryTreeNode readNode(int seqNumber) throws ClosedChannelException {
        /* Do a cache lookup */
        int offset = seqNumber & CACHE_MASK;
        HistoryTreeNode readNode = fNodeCache[offset];
        if (readNode != null && readNode.getSeqNumber() == seqNumber) {
            return readNode;
        }

        /* Lookup on disk */
        try {
            seekFCToNodePos(fFileChannelIn, seqNumber);
            readNode = HistoryTreeNode.readNode(fBlockSize, fMaxChildren, fFileChannelIn);

            /* Put the node in the cache. */
            fNodeCache[offset] = readNode;
            return readNode;

        } catch (ClosedChannelException e) {
            throw e;
        } catch (IOException e) {
            /* Other types of IOExceptions shouldn't happen at this point though */
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new IllegalStateException();
        }
    }

    public synchronized void writeNode(HistoryTreeNode node) {
        try {
            /* Insert the node into the cache. */
            int seqNumber = node.getSeqNumber();
            int offset = seqNumber & CACHE_MASK;
            fNodeCache[offset] = node;

            /* Position ourselves at the start of the node and write it */
            seekFCToNodePos(fFileChannelOut, seqNumber);
            node.writeSelf(fFileChannelOut);
        } catch (IOException e) {
            /* If we were able to open the file, we should be fine now... */
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public FileChannel getFcOut() {
        return fFileChannelOut;
    }

    public FileInputStream supplyATReader(int nodeOffset) {
        try {
            /*
             * Position ourselves at the start of the Mapping section in the
             * file (which is right after the Blocks)
             */
            seekFCToNodePos(fFileChannelIn, nodeOffset);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
        return fFileInputStream;
    }

    public synchronized void closeFile() {
        try {
            fFileInputStream.close();
            fFileOutputStream.close();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public synchronized void deleteFile() {
        closeFile();

        File historyTreeFile = fStateFile;
        if (!historyTreeFile.delete()) {
            /* We didn't succeed in deleting the file */
            LOGGER.severe("Failed to delete" + historyTreeFile.getName()); //$NON-NLS-1$
        }
    }

    /**
     * Seek the given FileChannel to the position corresponding to the node that
     * has seqNumber
     *
     * @param fc
     *            the channel to seek
     * @param seqNumber
     *            the node sequence number to seek the channel to
     * @throws IOException
     *             If some other I/O error occurs
     */
    private void seekFCToNodePos(FileChannel fc, int seqNumber)
            throws IOException {
        /*
         * Cast to (long) is needed to make sure the result is a long too and
         * doesn't get truncated
         */
        fc.position(HistoryTree.TREE_HEADER_SIZE
                + ((long) seqNumber) * fBlockSize);
    }

}
