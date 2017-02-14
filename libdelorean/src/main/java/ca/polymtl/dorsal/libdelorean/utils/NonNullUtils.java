/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 * Copyright (C) 2014-2015 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.utils;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.stream.Stream;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;

/**
 * Utility methods to handle {@link org.eclipse.jdt.annotation.NonNull}
 * annotations.
 *
 * @author Alexandre Montplaisir
 */
public final class NonNullUtils {

    private NonNullUtils() {
    }

    /**
     * Returns a non-null {@link String} for a potentially null object. This
     * method calls {@link Object#toString()} if the object is not null, or
     * returns an empty string otherwise.
     *
     * @param obj
     *            A {@link Nullable} object that we want converted to a string
     * @return The non-null string
     */
    public static String nullToEmptyString(@Nullable Object obj) {
        if (obj == null) {
            return ""; //$NON-NLS-1$
        }
        String str = obj.toString();
        return (str == null ? "" : str); //$NON-NLS-1$
    }

    // ------------------------------------------------------------------------
    // checkNotNull() methods, to convert @Nullable references to @NonNull ones
    // ------------------------------------------------------------------------

    /**
     * Ensures a {@link Stream} does not contain any null values.
     *
     * This also "upcasts" the reference from a Stream<@Nullable T> to a
     * Stream<@NonNull T>.
     *
     * @param stream
     *            The stream to check for
     * @return A stream with the same elements
     * @throws NullPointerException
     *             If the stream itself or any of its values are null
     */
    public static <T> Stream<@NonNull T> checkNotNullContents(@Nullable Stream<T> stream) {
        if (stream == null) {
            throw new NullPointerException();
        }
        Stream<@NonNull T> ret = stream.map(t -> requireNonNull(t));
        return ret;
    }

    /**
     * Ensures an array does not contain any null elements.
     *
     * @param array
     *            The array to check
     * @return The same array, now with guaranteed @NonNull elements
     * @throws NullPointerException
     *             If the array reference or any contained element was null
     */
    public static <T> @NonNull T[] checkNotNullContents(T @Nullable [] array) {
        if (array == null) {
            throw new NullPointerException();
        }
        Arrays.stream(array).forEach(elem -> requireNonNull(elem));
        @SuppressWarnings("null")
        @NonNull T[] ret = array;
        return ret;
    }
}
