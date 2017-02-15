/*
 * Copyright (C) 2016-2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.aggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import ca.polymtl.dorsal.libdelorean.ITmfStateSystemBuilder;
import ca.polymtl.dorsal.libdelorean.exceptions.AttributeNotFoundException;
import ca.polymtl.dorsal.libdelorean.exceptions.StateSystemDisposedException;
import ca.polymtl.dorsal.libdelorean.interval.ITmfStateInterval;
import ca.polymtl.dorsal.libdelorean.interval.TmfStateInterval;
import ca.polymtl.dorsal.libdelorean.statevalue.ITmfStateValue;
import ca.polymtl.dorsal.libdelorean.statevalue.TmfStateValue;

/**
 * Aggregation rule based on attribute priority.
 *
 * At construction, the caller provider a list of attribute paths. These
 * attributes will be used to populate the aggregate attribute. The order of
 * this list is important! The first non-null-value state will be used as value
 * for the aggregate state.
 *
 * If none of the pointed attributes are non-null, only then the reported value
 * will be a null value too.
 *
 * @author Alexandre Montplaisir
 */
public class AttributePriorityAggregationRule extends StateAggregationRule {

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
     *            The list of attributes (specified with their absolute paths)
     *            used to populate the aggregate. The earlier elements in the
     *            list are prioritized over the later ones if several are
     *            non-null.
     */
    public AttributePriorityAggregationRule(ITmfStateSystemBuilder ssb,
            int targetQuark,
            List<String[]> attributePatterns) {
        super(ssb, targetQuark, attributePatterns);
    }

    @Override
    public ITmfStateValue getOngoingAggregatedState() {
        Optional<ITmfStateValue> possibleValue = getQuarkStream()
                /* Query the value of each quark in the rule */
                .map(quark -> {
                    try {
                        return getStateSystem().queryOngoingState(quark.intValue());
                    } catch (AttributeNotFoundException e) {
                        throw new IllegalStateException("Bad aggregation rule"); //$NON-NLS-1$
                    }
                })
                /*
                 * The patterns should have been inserted in order of priority,
                 * the first non-null one is the one we want.
                 */
                .filter(value -> !value.isNull())
                .findFirst();

        return possibleValue.orElse(TmfStateValue.nullValue());
    }

    @Override
    public ITmfStateInterval getAggregatedState(long timestamp) {
        ITmfStateSystemBuilder ss = getStateSystem();

        /* First we need all the currently valid quarks */
        List<Integer> quarks = getQuarkStream().collect(Collectors.toList());

        /*
         * To determine the value, we will iterate through the corresponding
         * state intervals and keep the first non-null-value one.
         *
         * To determine the start/end times, we need to look through the subset
         * of intervals starting with the one we kept, plus all *higher
         * priority* ones. The lower-priority intervals cannot affect this
         * state, so they are ignored.
         */

        List<ITmfStateInterval> intervalsToUse = new ArrayList<>();
        ITmfStateValue value = TmfStateValue.nullValue();

        try {
            for (Integer quark : quarks) {
                ITmfStateInterval interval = ss.querySingleState(timestamp, quark);
                intervalsToUse.add(interval);

                ITmfStateValue sv = interval.getStateValue();
                if (!sv.isNull()) {
                    value = sv;
                    break;
                }
            }

            long start = intervalsToUse.stream()
                    .mapToLong(ITmfStateInterval::getStartTime)
                    .max().orElse(ss.getStartTime());

            long end = intervalsToUse.stream()
                    .mapToLong(ITmfStateInterval::getEndTime)
                    .min().orElse(ss.getCurrentEndTime());

            return new TmfStateInterval(start, end, getTargetQuark(), value);

        } catch (AttributeNotFoundException | StateSystemDisposedException e) {
            throw new IllegalStateException("Bad aggregation rule"); //$NON-NLS-1$
        }
    }

}