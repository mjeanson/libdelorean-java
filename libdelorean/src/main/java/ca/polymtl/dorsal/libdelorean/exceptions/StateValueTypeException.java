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

package ca.polymtl.dorsal.libdelorean.exceptions;

import ca.polymtl.dorsal.libdelorean.statevalue.ITmfStateValue;

/**
 * The StateValue is a wrapper around the different type of values that can be
 * used and stored in the state system and history. "Unboxing" the value means
 * retrieving the base type (int, String, etc.) inside it.
 *
 * This exception is thrown if the user tries to unbox a StateValue with an
 * incorrect type (for example, tries to read a String value as an Int).
 *
 * To avoid it, always check for the state value's type before attempting to
 * unbox it, via {@link ITmfStateValue#getType()}.
 *
 * @author Alexandre Montplaisir
 */
public class StateValueTypeException extends RuntimeException {

    private static final long serialVersionUID = -4548793451746144513L;

    /**
     * Default constructor
     */
    public StateValueTypeException() {
        super();
    }

    /**
     * Constructor with a message
     *
     * @param message
     *            Message to attach to this exception
     */
    public StateValueTypeException(String message) {
        super(message);
    }

    /**
     * Constructor with both a message and a cause.
     *
     * @param message
     *            Message to attach to this exception
     * @param e
     *            Cause of this exception
     */
    public StateValueTypeException(String message, Throwable e) {
        super(message, e);
    }
}
