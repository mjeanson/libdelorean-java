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

import ca.polymtl.dorsal.libdelorean.exceptions.TimeRangeException;
import ca.polymtl.dorsal.libdelorean.interval.IStateInterval;
import ca.polymtl.dorsal.libdelorean.statevalue.IStateValue;
import ca.polymtl.dorsal.libdelorean.statevalue.StateValue;
import org.eclipse.jdt.annotation.NonNull;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The interval component, which will be contained in a node of the History
 * Tree.
 *
 * @author Alexandre Montplaisir
 */
public final class HTInterval implements IStateInterval, Comparable<HTInterval> {

    private static final String errMsg = "Invalid interval data. Maybe your file is corrupt?"; //$NON-NLS-1$

    /* 'Byte' equivalent for state values types */
    private static final byte TYPE_NULL = -1;
    private static final byte TYPE_INTEGER = 0;
    private static final byte TYPE_STRING = 1;
    private static final byte TYPE_LONG = 2;
    private static final byte TYPE_DOUBLE = 3;
    private static final byte TYPE_BOOLEAN_TRUE = 4;
    private static final byte TYPE_BOOLEAN_FALSE = 5;

    private final long start;
    private final long end;
    private final int attribute;
    private final @NonNull StateValue sv;

    /** Size of this interval once serialized to disk (in bytes) */
    private final transient int sizeOnDisk;

    /**
     * Standard constructor
     *
     * @param intervalStart
     *            Start time of the interval
     * @param intervalEnd
     *            End time of the interval
     * @param attribute
     *            Attribute (quark) to which the state represented by this
     *            interval belongs
     * @param value
     *            State value represented by this interval
     * @throws TimeRangeException
     *             If the start time or end time are invalid
     */
    public HTInterval(long intervalStart, long intervalEnd, int attribute,
            @NonNull StateValue value) throws TimeRangeException {
        if (intervalStart > intervalEnd) {
            throw new TimeRangeException("Start:" + intervalStart + ", End:" + intervalEnd); //$NON-NLS-1$ //$NON-NLS-2$
        }

        this.start = intervalStart;
        this.end = intervalEnd;
        this.attribute = attribute;
        this.sv = value;
        this.sizeOnDisk = computeSizeOnDisk(sv);

        /* We only support values up to 2^16 in length */
        if (sizeOnDisk > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Interval is too large for the state system: " + this.toString()); //$NON-NLS-1$
        }
    }

    /**
     * Compute how much space (in bytes) an interval will take in its serialized
     * form on disk. This is dependent on its state value.
     */
    private static int computeSizeOnDisk(IStateValue sv) {
        /*
         * Minimum size is 2x long (start and end), 1x int (attribute) and 1x
         * byte (value type).
         */
        final int minSize = Long.BYTES + Long.BYTES + Integer.BYTES + Byte.BYTES;

        switch (sv.getType()) {
        case NULL:
        case BOOLEAN:
            return minSize;
        case INTEGER:
            return (minSize + Integer.BYTES);
        case LONG:
            return (minSize + Long.BYTES);
        case DOUBLE:
            return (minSize + Double.BYTES);
        case STRING:
            /*
             * String's length + 3 (2 bytes for size, 1 byte for \0 at the end
             */
            return (minSize + sv.unboxStr().getBytes().length + 3);
        default:
            /*
             * It's very important that we know how to write the state value in
             * the file!!
             */
            throw new IllegalStateException();
        }
    }

    /**
     * "Faster" constructor for inner use only.
     *
     * When we build an interval by reading it from disk with {@link #readFrom},
     * we already know its serialized size, so there is no need to call
     * computeStringsEntrySize() and potentially have to serialize a
     * string state value again.
     */
    private HTInterval(long intervalStart, long intervalEnd, int attribute,
                       @NonNull StateValue value, int size) throws TimeRangeException {
        if (intervalStart > intervalEnd) {
            throw new TimeRangeException("Start:" + intervalStart + ", End:" + intervalEnd); //$NON-NLS-1$ //$NON-NLS-2$
        }

        this.start = intervalStart;
        this.end = intervalEnd;
        this.attribute = attribute;
        this.sv = value;
        this.sizeOnDisk = size;
    }

    /**
     * Reader factory method. Builds the interval using an already-allocated
     * ByteBuffer, which normally comes from a NIO FileChannel.
     *
     * @param buffer
     *            The ByteBuffer from which to read the information
     * @return The interval object
     * @throws IOException
     *             If there was an error reading from the buffer
     */
    public static final HTInterval readFrom(ByteBuffer buffer) throws IOException {
        StateValue value;

        int startPos = buffer.position();

        /* Read the data common to all intervals */
        byte valueType = buffer.get();
        long intervalStart = buffer.getLong();
        long intervalEnd = buffer.getLong();
        int attribute = buffer.getInt();

        switch (valueType) {
        case TYPE_NULL:
            value = StateValue.nullValue();
            break;

        case TYPE_BOOLEAN_TRUE:
            value = StateValue.newValueBoolean(true);
            break;

        case TYPE_BOOLEAN_FALSE:
            value = StateValue.newValueBoolean(false);
            break;

        case TYPE_INTEGER:
            value = StateValue.newValueInt(buffer.getInt());
            break;

        case TYPE_LONG:
            value = StateValue.newValueLong(buffer.getLong());
            break;

        case TYPE_DOUBLE:
            value = StateValue.newValueDouble(buffer.getDouble());
            break;

        case TYPE_STRING: {
            /* the first byte = the size to read */
            int strSize = buffer.getShort();
            byte[] array = new byte[strSize];
            buffer.get(array);
            value = StateValue.newValueString(new String(array));

            /* Confirm the 0'ed byte at the end */
            byte res = buffer.get();
            if (res != 0) {
                throw new IOException(errMsg);
            }
        }
            break;

        default:
            /* Unknown data, better to not make anything up... */
            throw new IOException(errMsg);
        }

        try {
            int intervalSize = buffer.position() - startPos;
            HTInterval interval = new HTInterval(intervalStart, intervalEnd, attribute, value, intervalSize);
            return interval;
        } catch (TimeRangeException e) {
            throw new IOException(errMsg);
        }

    }

    /**
     * Antagonist of the previous constructor, write the Data entry
     * corresponding to this interval in a ByteBuffer (mapped to a block in the
     * history-file, hopefully)
     *
     * @param buffer
     *            The already-allocated ByteBuffer corresponding to a SHT Node
     */
    public void writeInterval(ByteBuffer buffer) {
        int startPos = buffer.position();

        final byte typeByte = getByteFromType(sv);

        buffer.put(typeByte);
        buffer.putLong(start);
        buffer.putLong(end);
        buffer.putInt(attribute);

        switch (typeByte) {
        case TYPE_NULL:
        case TYPE_BOOLEAN_TRUE:
        case TYPE_BOOLEAN_FALSE:
            /* Nothing else to write, 'typeByte' carries all the information */
            break;
        case TYPE_INTEGER:
            buffer.putInt(sv.unboxInt());
            break;

        case TYPE_LONG:
            buffer.putLong(sv.unboxLong());
            break;

        case TYPE_DOUBLE:
            buffer.putDouble(sv.unboxDouble());
            break;

        case TYPE_STRING:
            String string = sv.unboxStr();
            byte[] strArray = string.getBytes();

            /* Write the string size, then the actual bytes, then \0 */
            buffer.putShort((short) strArray.length);
            buffer.put(strArray);
            buffer.put((byte) 0);

            break;

        default:
            break;
        }

        int written = buffer.position() - startPos;
        if (written != sizeOnDisk) {
            throw new IllegalStateException("Did not write the expected amount of bytes when serializing interval."); //$NON-NLS-1$
        }
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
    public IStateValue getStateValue() {
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

    public int getSizeOnDisk() {
        return sizeOnDisk;
    }

    /**
     * Compare the END TIMES of different intervals. This is used to sort the
     * intervals when we close down a node.
     */
    @Override
    public int compareTo(HTInterval other) {
        if (this.end < other.end) {
            return -1;
        } else if (this.end > other.end) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof HTInterval &&
                this.compareTo((HTInterval) other) == 0) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        /* Only for debug, should not be externalized */
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        sb.append(start);
        sb.append(", "); //$NON-NLS-1$
        sb.append(end);
        sb.append(']');

        sb.append(", attribute = "); //$NON-NLS-1$
        sb.append(attribute);

        sb.append(", value = "); //$NON-NLS-1$
        sb.append(sv.toString());

        return sb.toString();
    }

    /**
     * Here we determine how state values "types" are written in the 8-bit field
     * that indicates the value type in the file.
     */
    private static byte getByteFromType(IStateValue sv) {
        switch(sv.getType()) {
        case NULL:
            return TYPE_NULL;
        case BOOLEAN:
            return (sv.unboxBoolean() ? TYPE_BOOLEAN_TRUE : TYPE_BOOLEAN_FALSE);
        case INTEGER:
            return TYPE_INTEGER;
        case STRING:
            return TYPE_STRING;
        case LONG:
            return TYPE_LONG;
        case DOUBLE:
            return TYPE_DOUBLE;
        default:
            /* Should not happen if the switch is fully covered */
            throw new IllegalStateException();
        }
    }
}
