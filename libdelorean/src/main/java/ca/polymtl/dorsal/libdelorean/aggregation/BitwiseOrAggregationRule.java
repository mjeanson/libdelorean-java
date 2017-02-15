/*
 * Copyright (C) 2016-2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.aggregation;

import java.util.List;
import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.stream.Stream;

import ca.polymtl.dorsal.libdelorean.ITmfStateSystemBuilder;
import ca.polymtl.dorsal.libdelorean.exceptions.AttributeNotFoundException;
import ca.polymtl.dorsal.libdelorean.exceptions.StateSystemDisposedException;
import ca.polymtl.dorsal.libdelorean.interval.ITmfStateInterval;
import ca.polymtl.dorsal.libdelorean.interval.TmfStateInterval;
import ca.polymtl.dorsal.libdelorean.statevalue.ITmfStateValue;
import ca.polymtl.dorsal.libdelorean.statevalue.TmfStateValue;

/**
 * Aggregation rule that does bitwise-OR operations of all specified attributes.
 *
 * Will only work with attributes storing Integer state values.
 *
 * @author Alexandre Montplaisir
 */
public class BitwiseOrAggregationRule extends StateAggregationRule {

    /**
     * Constructor
     *
     * Don't forget to also register this rule to the provided state system,
     * using {@link ITmfStateSystemBuilder#addAggregationRule}.
     *
     * @param ssb
     *            The state system on which this rule will be associated.
     * @param targetQuark
     *            The aggregate quark where this rule will be "mounted"
     * @param attributePatterns
     *            The attributes (specified with their absolute paths) used to
     *            populate the aggregate. The order of the elements is not
     *            important.
     */
    public BitwiseOrAggregationRule(ITmfStateSystemBuilder ssb,
            int targetQuark,
            List<String[]> attributePatterns) {
        super(ssb, targetQuark, attributePatterns);
    }

    @Override
    public ITmfStateValue getOngoingAggregatedState() {
        OptionalInt value = getQuarkStream()
                /* Query the value of each quark in the rule */
                .map(quark -> {
                        try {
                            return getStateSystem().queryOngoingState(quark.intValue());
                        } catch (AttributeNotFoundException e) {
                            throw new IllegalStateException("Bad aggregation rule"); //$NON-NLS-1$
                        }
                    })
                .filter(stateValue -> !stateValue.isNull())
                .mapToInt(stateValue -> stateValue.unboxInt())
                .reduce((a, b) -> a | b);

        if (value.isPresent()) {
            return TmfStateValue.newValueInt(value.getAsInt());
        }
        return TmfStateValue.nullValue();
    }

    @Override
    public ITmfStateInterval getAggregatedState(long timestamp) {
        ITmfStateSystemBuilder ss = getStateSystem();

        /* We first need to get all the valid state intervals */
        Supplier<Stream<ITmfStateInterval>> intervals = () -> (getQuarkStream()
                .map(quark -> {
                        try {
                            return ss.querySingleState(timestamp, quark.intValue());
                        } catch (AttributeNotFoundException | StateSystemDisposedException e) {
                            throw new IllegalStateException("Bad aggregation rule"); //$NON-NLS-1$
                        }
                    })
                );

        /* Calculate the value */
        OptionalInt possibleValue = intervals.get()
                .filter(stateInterval -> !stateInterval.getStateValue().isNull())
                .mapToInt(stateInterval -> stateInterval.getStateValue().unboxInt())
                .reduce((a, b) -> a | b);

        ITmfStateValue value = (possibleValue.isPresent() ?
                TmfStateValue.newValueInt(possibleValue.getAsInt()) :
                TmfStateValue.nullValue());

        /* Calculate the dummy interval start (the latest one) */
        long start = intervals.get()
                .mapToLong(ITmfStateInterval::getStartTime)
                .max().orElse(ss.getStartTime());

        /* Calculate the dummy interval end (the earliest one) */
        long end = intervals.get()
                .mapToLong(ITmfStateInterval::getEndTime)
                .min().orElse(ss.getCurrentEndTime());

        return new TmfStateInterval(start, end, getTargetQuark(), value);
    }

}