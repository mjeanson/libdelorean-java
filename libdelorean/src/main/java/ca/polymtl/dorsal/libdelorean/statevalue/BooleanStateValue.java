/*
 * Copyright (C) 2016-2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.statevalue;

import java.util.Objects;

import org.eclipse.jdt.annotation.Nullable;

import ca.polymtl.dorsal.libdelorean.exceptions.StateValueTypeException;

/**
 * A state value containing a boolean primitive.
 *
 * @author Alexandre Montplaisir
 */
final class BooleanStateValue extends StateValue {

    private final boolean value;

    public BooleanStateValue(boolean value) {
        this.value = value;
    }

    @Override
    public Type getType() {
        return Type.BOOLEAN;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean equals(@Nullable Object object) {
        if (!(object instanceof BooleanStateValue)) {
            return false;
        }
        BooleanStateValue other = (BooleanStateValue) object;
        return (this.value == other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public @Nullable String toString() {
        return Boolean.toString(value);
    }

    // ------------------------------------------------------------------------
    // Unboxing methods
    // ------------------------------------------------------------------------

    @Override
    public boolean unboxBoolean() {
        return value;
    }

    @Override
    public int compareTo(@Nullable IStateValue other) {
        if (other == null) {
            throw new IllegalArgumentException("Cannot compare against null reference."); //$NON-NLS-1$
        }

        if (other.getType() == Type.NULL) {
            /* We consider null values to be "smaller" than everything else. */
            return 1;
        } else if (other.getType() == Type.BOOLEAN) {
            return Boolean.compare(value, ((BooleanStateValue) other).value);
        } else {
            throw new StateValueTypeException("A boolean state value cannot be compared to the type " + other.getType()); //$NON-NLS-1$
        }
    }

}