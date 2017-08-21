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

package ca.polymtl.dorsal.libdelorean.backend.historytree

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.nio.channels.ClosedChannelException
import java.nio.channels.FileChannel
import java.util.logging.Level
import java.util.logging.Logger

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
internal class HT_IO(private val stateFile: File,
                     private val blockSize: Int,
                     private val maxChildren: Int,
                     newFile: Boolean) {

    companion object {
        private val LOGGER = Logger.getLogger(HT_IO::class.java.name)

        /**
         * Cache size, must be a power of 2
         * TODO test/benchmark optimal cache size
         */
        private const val CACHE_SIZE = 256
        private const val CACHE_MASK = CACHE_SIZE - 1
    }

    /* Properties related to file I/O */
    private val fis: FileInputStream
    private val fos: FileOutputStream
    private val fcIn: FileChannel
    val fcOut: FileChannel

    private val nodeCache: Array<HistoryTreeNode?> = arrayOfNulls(CACHE_SIZE)

    init {
        if (newFile) {
            var success1 = true
            /* Create a new empty History Tree file */
            if (stateFile.exists()) {
                success1 = stateFile.delete()
            }
            val success2 = stateFile.createNewFile()
            if (!(success1 && success2)) {
                /* It seems we do not have permission to create the new file */
                throw IOException("Cannot create new file at ${stateFile.name}")
            }
            fis = FileInputStream(stateFile)
            fos = FileOutputStream(stateFile, false)
        } else {
            /*
             * We want to open an existing file, make sure we don't squash the
             * existing content when opening the fos!
             */
            fis = FileInputStream(stateFile)
            fos = FileOutputStream(stateFile, true)
        }
        fcIn = fis.channel
        fcOut = fos.channel
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
    @Synchronized
    fun readNode(seqNumber: Int): HistoryTreeNode {
        /* Do a cache lookup */
        val offset = (seqNumber and CACHE_MASK)
        var readNode = nodeCache[offset]
        if (readNode != null && readNode.seqNumber == seqNumber) {
            return readNode
        }

        /* Lookup on disk */
        try {
            seekFCToNodePos(fcIn, seqNumber)
            readNode = HistoryTreeNode.readNode(blockSize, maxChildren, fcIn)

            /* Put the node in the cache. */
            nodeCache[offset] = readNode
            return readNode

        } catch (e: ClosedChannelException) {
            throw e
        } catch (e: IOException) {
            /* Other types of IOExceptions shouldn't happen at this point though */
            LOGGER.log(Level.SEVERE, e.message, e)
            throw IllegalStateException()
        }
    }

    @Synchronized
    fun writeNode(node: HistoryTreeNode) {
        try {
            /* Insert the node into the cache. */
            val seqNumber = node.seqNumber
            val offset = (seqNumber and CACHE_MASK)
            nodeCache[offset] = node

            /* Position ourselves at the start of the node and write it */
            seekFCToNodePos(fcOut, seqNumber)
            node.writeSelf(fcOut)

        } catch (e: IOException) {
            /* If we were able to open the file, we should be fine now... */
            LOGGER.log(Level.SEVERE, e.message, e)
        }
    }

    fun supplyATReader(nodeOffset: Int): FileInputStream {
        try {
            /*
             * Position ourselves at the start of the Mapping section in the
             * file (which is right after the Blocks)
             */
            seekFCToNodePos(fcIn, nodeOffset)
        } catch (e: IOException) {
            LOGGER.log(Level.SEVERE, e.message, e)
        }
        return fis
    }

    @Synchronized
    fun closeFile() {
        try {
            fis.close()
            fos.close()
        } catch (e: IOException) {
            LOGGER.log(Level.SEVERE, e.message, e)
        }
    }

    @Synchronized
    fun deleteFile() {
        closeFile()

        if (!stateFile.delete()) {
            /* We didn't succeed in deleting the file */
            LOGGER.severe("Failed to delete" + stateFile.name)
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
    private fun seekFCToNodePos(fc: FileChannel, seqNumber: Int) {
        /*
         * Cast to (long) is needed to make sure the result is a long too and
         * doesn't get truncated
         */
        fc.position(HistoryTree.TREE_HEADER_SIZE
                + seqNumber.toLong() * blockSize)
    }
}