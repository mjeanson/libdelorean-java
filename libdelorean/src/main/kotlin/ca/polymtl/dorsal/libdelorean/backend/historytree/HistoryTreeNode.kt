/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.backend.historytree

import ca.polymtl.dorsal.libdelorean.interval.IStateInterval
import ca.polymtl.dorsal.libdelorean.statevalue.StateValue
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * <pre>
 *  1 - byte (type)
 * 16 - 2x long (start time, end time)
 * 16 - 4x int (seq number, parent seq number, intervalcount,
 *              strings section pos.)
 *  1 - byte (done or not)
 * </pre>
 */
private const val COMMON_HEADER_SIZE = 34;

/**
 * The base class for all the types of nodes that go in the History Tree.
 *
 * @author Alexandre Montplaisir
 */
sealed class HistoryTreeNode(val blockSize: Int,
                             val seqNumber: Int,
                             var parentSeqNumber: Int,
                             val nodeStart: Long) {

    var nodeEnd: Long? = null
        private set

    /* Sum of bytes of all intervals in the node */
    private var sizeOfIntervalSection = 0

    /* True if this node was read from disk (meaning its end time is now fixed) */
    @Volatile
    var isOnDisk = false
        private set

    /* Vector containing all the intervals contained in this node */
    val intervals = mutableListOf<HTInterval>()

    /* Lock used to protect the accesses to intervals, nodeEnd and such */
    private val rwl = ReentrantReadWriteLock(false)

    companion object {
        /**
         * Reader factory method. Build a Node object (of the right type) by reading
         * a block in the file.
         *
         * @param blockSize
         *            The size of each "block" on disk. One node will always fit in
         *            one block.
         * @param maxChildren
         *            The maximum number of children allowed per core (non-leaf)
         *            node.
         * @param fc
         *            FileChannel to the history file, ALREADY SEEKED at the start
         *            of the node.
         * @return The node object
         * @throws IOException
         *             If there was an error reading from the file channel
         */
        @JvmStatic
        fun readNode(blockSize: Int, maxChildren: Int, fc: FileChannel): HistoryTreeNode {
            val buffer = ByteBuffer.allocate(blockSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.clear();
            val res = fc.read(buffer);
            if (res != blockSize) throw IOException()
            buffer.flip();

            /* Read the common header part */
            val typeByte = buffer.get();
            val start = buffer.getLong();
            val end = buffer.getLong();
            val seqNb = buffer.getInt();
            val parentSeqNb = buffer.getInt();
            val intervalCount = buffer.getInt();

            /* Now the rest of the header depends on the node type */
            val newNode = when (typeByte) {
                CoreNode.CORE_TYPE_BYTE -> CoreNode(blockSize, maxChildren, seqNb, parentSeqNb, start)
                LeafNode.LEAF_TYPE_BYTE -> LeafNode(blockSize, seqNb, parentSeqNb, start)
                else -> throw IOException()
            }
            newNode.readSpecificHeader(buffer);

            /*
             * At this point, we should be done reading the header and 'buffer'
             * should only have the intervals left
             */
            (0 until intervalCount).forEach {
                val interval = HTInterval.readFrom(buffer);
                newNode.intervals.add(interval);
                newNode.sizeOfIntervalSection += interval.sizeOnDisk;
            }

            /* Assign the node's other information we have read previously */
            newNode.nodeEnd = end;
            newNode.isOnDisk = true;

            return newNode;
        }
    }

    /**
     * Write this node to the given file channel.
     */
    fun writeSelf(fc: FileChannel) {
        /*
         * Yes, we are taking the *read* lock here, because we are reading the
         * information in the node to write it to disk.
         */
        rwl.readLock().lock();
        try {
            val buffer = ByteBuffer.allocate(blockSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.clear();

            /* Write the common header part */
            buffer.put(nodeByte);
            buffer.putLong(nodeStart);
            buffer.putLong(nodeEnd ?: 0L);
            buffer.putInt(seqNumber);
            buffer.putInt(parentSeqNumber);
            buffer.putInt(intervals.size);

            /* Now call the inner method to write the specific header part */
            writeSpecificHeader(buffer);

            /* Back to us, we write the intervals */
            intervals.forEach { it.writeInterval(buffer) }

            /* Fill the rest of the block with zeroes. */
            while (buffer.position() < blockSize) {
                buffer.put(0.toByte())
            }

            /* Finally, write everything in the Buffer to disk */
            buffer.flip();
            val res = fc.write(buffer);
            if (res != blockSize) {
                throw IllegalStateException("Wrong size of block written: Actual: " + res + ", Expected: " + blockSize); //$NON-NLS-1$ //$NON-NLS-2$
            }

        } finally {
            rwl.readLock().unlock();
        }
        isOnDisk = true;
    }

    fun takeReadLock() {
        rwl.readLock().lock();
    }

    fun releaseReadLock() {
        rwl.readLock().unlock();
    }

    /**
     * Add an interval to this node
     */
    fun addInterval(newInterval: HTInterval) {
        rwl.writeLock().lock()
        try {
            /* Just in case, should be checked before even calling this function */
            assert (newInterval.sizeOnDisk <= nodeFreeSpace)

            /* Find the insert position to keep the list sorted */
            var index = intervals.size
            while (index > 0 && newInterval < intervals[index - 1]) {
                index--
            }

            intervals.add(index, newInterval)
            sizeOfIntervalSection += newInterval.sizeOnDisk

        } finally {
            rwl.writeLock().unlock()
        }
    }

    /**
     * We've received word from the containerTree that newest nodes now exist to
     * our right. (Puts isDone = true and sets the endtime)
     */
    fun closeThisNode(endTime: Long) {
        rwl.writeLock().lock();
        try {
            /**
             * FIXME: was assert (endtime >= fNodeStart); but that exception
             * is reached with an empty node that has start time endtime + 1
             */
//            if (endtime < fNodeStart) {
//                throw new IllegalArgumentException("Endtime " + endtime + " cannot be lower than start time " + fNodeStart);
//            }

            if (intervals.isNotEmpty()) {
                /*
                 * Make sure there are no intervals in this node with their
                 * EndTime > the one requested. Only need to check the last one
                 * since they are sorted
                 */
                if (endTime < intervals.last().endTime) {
                    throw IllegalArgumentException("Closing end time should be greater than or equal to the end time of the intervals of this node"); //$NON-NLS-1$
                }
            }
            nodeEnd = endTime;

        } finally {
            rwl.writeLock().unlock();
        }
    }

    /**
     * The method to fill up the stateInfo (passed on from the Current State
     * Tree when it does a query on the SHT). We'll replace the data in that
     * vector with whatever relevant we can find from this node
     *
     * @param stateInfo
     *            The same stateInfo that comes from SHT's doQuery()
     * @param t
     *            The timestamp for which the query is for. Only return
     *            intervals that intersect t.
     */
    fun writeInfoFromNode(stateInfo: MutableList<IStateInterval>, t: Long) {
        /* This is from a state system query, we are "reading" this node */
        rwl.readLock().lock();
        try {
            (getStartIndexFor(t) until intervals.size)
                    .map { intervals[it] }
                    /*
                     * Second condition is to ignore new attributes that might have
                     * been created after stateInfo was instantiated (they would be
                     * null anyway).
                     */
                    .filter { it.startTime <= t && it.attribute < stateInfo.size }
                    .forEach { stateInfo[it.attribute] = it }
        } finally {
            rwl.readLock().unlock();
        }
    }

    /**
     * Get a single Interval from the information in this node If the
     * key/timestamp pair cannot be found, we return null.
     *
     * @param key
     *            The attribute quark to look for
     * @param t
     *            The timestamp
     * @return The Interval containing the information we want, or null if it
     *         wasn't found
     */
    fun getRelevantInterval(key: Int, t: Long): HTInterval? {
        rwl.readLock().lock();
        try {
            return (getStartIndexFor(t) until intervals.size)
                    .map { intervals[it] }
                    .firstOrNull { it.attribute == key && it.intersects(t) };

        } finally {
            rwl.readLock().unlock();
        }
    }


    private fun getStartIndexFor(t: Long): Int {
        /* Should only be called by methods with the readLock taken */
        if (intervals.isEmpty()) {
            return 0
        }
        /*
         * Since the intervals are sorted by end time, we can skip all the ones
         * at the beginning whose end times are smaller than 't'. Java does
         * provides a .binarySearch method, but its API is quite weird...
         */
        val dummy = HTInterval(0, t, 0, StateValue.nullValue());
        var index = Collections.binarySearch(intervals, dummy);

        if (index < 0) {
            /*
             * .binarySearch returns a negative number if the exact value was
             * not found. Here we just want to know where to start searching, we
             * don't care if the value is exact or not.
             */
            index = -index - 1;

        } else {
            /*
             * Another API quirkiness, the returned index is the one of the *last*
             * element of a series of equal endtimes, which happens sometimes. We
             * want the *first* element of such a series, to read through them
             * again.
             */
            while (index > 0
                    && intervals[index - 1].compareTo(intervals[index]) == 0) {
                index--;
            }
        }

        return index;
    }

    val totalHeaderSize get() = COMMON_HEADER_SIZE + specificHeaderSize
    private val dataSectionEndOffset get() = totalHeaderSize + sizeOfIntervalSection

    val nodeFreeSpace: Int
        get() {
            rwl.readLock().lock()
            val ret = blockSize - dataSectionEndOffset
            rwl.readLock().unlock()
            return ret
        }

    protected abstract val nodeByte: Byte
    protected abstract val specificHeaderSize: Int

    protected abstract fun readSpecificHeader(buffer: ByteBuffer)
    protected abstract fun writeSpecificHeader(buffer: ByteBuffer)
}



private class CoreNode(blockSize: Int,
                        val maxChildren: Int,
                        seqNumber: Int,
                        parentSeqNumber: Int,
                        nodeStart: Long) : HistoryTreeNode(blockSize, seqNumber, parentSeqNumber, nodeStart) {

    companion object {
        const val CORE_TYPE_BYTE: Byte = 1
    }

    /** Nb. of children this node has */
    var nbChildren = 0
        private set

    /** Seq. numbers of the children nodes */
    private val children = IntArray(maxChildren)

    /** Start times of each of the children */
    private val childStart = LongArray(maxChildren)

    /** Seq number of this node's extension. -1 if none. Unused for now */
    private val extension = -1;

    /**
     * Lock used to gate the accesses to the children arrays. Meant to be a
     * different lock from the one in {@link HTNode}.
     */
    private val rwl = ReentrantReadWriteLock(false);

    fun getChild(index: Int): Int {
        rwl.readLock().lock();
        try {
            return children[index];
        } finally {
            rwl.readLock().unlock();
        }
    }

    fun getLatestChild(): Int {
        rwl.readLock().lock();
        try {
            return children.last()
        } finally {
            rwl.readLock().unlock();
        }
    }

    fun getChildStart(index: Int): Long {
        rwl.readLock().lock();
        try {
            return childStart[index];
        } finally {
            rwl.readLock().unlock();
        }
    }

    fun getLatestChildStart(): Long {
        rwl.readLock().lock();
        try {
            return childStart.last()
        } finally {
            rwl.readLock().unlock();
        }
    }

    fun linkNewChild(childNode: HistoryTreeNode) {
        rwl.writeLock().lock();
        try {
            if (nbChildren >= maxChildren) throw IllegalStateException()
            children[nbChildren] = childNode.seqNumber
            childStart[nbChildren] = childNode.nodeStart
            nbChildren++;

        } finally {
            rwl.writeLock().unlock();
        }
    }

    override val nodeByte = CORE_TYPE_BYTE
    override val specificHeaderSize: Int = (
                    /* 1x int (extension node) */
                    Integer.BYTES +
                    /* 1x int (nbChildren) */
                    Integer.BYTES +
                    /* MAX_NB * int ('children' table) */
                    Integer.BYTES * maxChildren +
                    /* MAX_NB * Timevalue ('childStart' table) */
                    java.lang.Long.BYTES * maxChildren)

    override fun readSpecificHeader(buffer: ByteBuffer) {
        /* Unused "extension", should be -1 */
        buffer.getInt();

        nbChildren = buffer.getInt();

        (0 until nbChildren).forEach { children[it] = buffer.getInt() }
        (nbChildren until maxChildren).forEach { buffer.getInt() }

        (0 until nbChildren).forEach { childStart[it] = buffer.getLong() }
        (nbChildren until maxChildren).forEach { buffer.getLong() }
    }

    override fun writeSpecificHeader(buffer: ByteBuffer) {
        buffer.putInt(extension);
        buffer.putInt(nbChildren);

        /* Write the "children's seq number" array */
        (0 until nbChildren).forEach { buffer.putInt(children[it]) }
        (nbChildren until maxChildren).forEach { buffer.putInt(0) }

        /* Write the "children's start times" array */
        (0 until nbChildren).forEach { buffer.putLong(childStart[it]) }
        (nbChildren until maxChildren).forEach { buffer.putLong(0L) }
    }

}

private class LeafNode(blockSize: Int,
                       seqNumber: Int,
                       parentSeqNumber: Int,
                       nodeStart: Long) : HistoryTreeNode(blockSize, seqNumber, parentSeqNumber, nodeStart) {

    companion object {
        const val LEAF_TYPE_BYTE: Byte = 2
    }

    override val nodeByte = LEAF_TYPE_BYTE
    override val specificHeaderSize: Int = 0 /* Empty */

    override fun readSpecificHeader(buffer: ByteBuffer) {
        /* No specific header part */
    }

    override fun writeSpecificHeader(buffer: ByteBuffer) {
        /* No specific header part */
    }
}