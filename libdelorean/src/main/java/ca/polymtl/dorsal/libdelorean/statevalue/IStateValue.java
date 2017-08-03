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

package ca.polymtl.dorsal.libdelorean.statevalue;

import ca.polymtl.dorsal.libdelorean.exceptions.StateValueTypeException;

/**
 * This is the interface for using state values and reading their contents.
 *
 * @author Alexandre Montplaisir
 */
public interface IStateValue extends Comparable<IStateValue> {

    /**
     * The supported types of state values
     */
    public enum Type {
        /** Null value, for an interval not carrying any information */
        NULL,
        /** Boolean value */
        BOOLEAN,
        /** 32-bit integer value */
        INTEGER,
        /** 64-bit integer value */
        LONG,
        /** IEEE 754 double precision number */
        DOUBLE,
        /** Variable-length string value */
        STRING,
    }

    /**
     * Each implementation has to define which one (among the supported types)
     * they implement. There could be more than one implementation of each type,
     * depending on the needs of the different users.
     *
     * @return The ITmfStateValue.Type enum representing the type of this value
     */
    Type getType();

    /**
     * Only "null values" should return true here
     *
     * @return True if this type of SV is considered "null", false if it
     *         contains a real value.
     */
    boolean isNull();

    /**
     * Read the contained value as a 'boolean' primitive.
     *
     * @return The boolean contained in the state value
     * @throws StateValueTypeException
     *             If the contained value cannot be read as a boolean
     */
    default boolean unboxBoolean() {
        throw new StateValueTypeException("Type " + getClass().getSimpleName() + //$NON-NLS-1$
                " cannot be unboxed into a boolean value."); //$NON-NLS-1$
    }

    /**
     * Read the contained value as an 'int' primitive
     *
     * @return The integer contained in the state value
     * @throws StateValueTypeException
     *             If the contained value cannot be read as an integer
     */
    int unboxInt();

    /**
     * Read the contained value as a 'long' primitive
     *
     * @return The long contained in the state value
     * @throws StateValueTypeException
     *             If the contained value cannot be read as a long
     */
    long unboxLong();

    /**
     * Read the contained value as a 'double' primitive
     *
     * @return The double contained in the state value
     * @throws StateValueTypeException
     *             If the contained value cannot be read as a double
     */
    double unboxDouble();

    /**
     * Read the contained value as a String
     *
     * @return The String contained in the state value
     * @throws StateValueTypeException
     *             If the contained value cannot be read as a String
     */
    String unboxStr();

}