/*
 * Copyright (C) 2016-2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.aggregation;

import java.util.Collections;
import java.util.OptionalInt;

import ca.polymtl.dorsal.libdelorean.ITmfStateSystem;
import ca.polymtl.dorsal.libdelorean.ITmfStateSystemBuilder;
import ca.polymtl.dorsal.libdelorean.exceptions.AttributeNotFoundException;
import ca.polymtl.dorsal.libdelorean.exceptions.StateSystemDisposedException;
import ca.polymtl.dorsal.libdelorean.interval.ITmfStateInterval;
import ca.polymtl.dorsal.libdelorean.interval.TmfStateInterval;
import ca.polymtl.dorsal.libdelorean.statevalue.ITmfStateValue;
import ca.polymtl.dorsal.libdelorean.statevalue.TmfStateValue;

/**
 * Simple aggregation that simply redirects to another attribute. Similar to a
 * symbolic link on a file system.
 *
 * @author Alexandre Montplaisir
 */
public class SymbolicLinkRule extends StateAggregationRule {

    /**
     * Constructor
     *
     * Don't forget to also register this rule to the provided state system,
     * using {@link ITmfStateSystemBuilder#addAggregationRule}.
     *
     * @param ssb
     *            The state system on which this rule will be associated.
     * @param quark
     *            The quark where this rule will be "mounted"
     * @param attributePattern
     *            The absolute path of the attribute to use as a target for the
     *            symlink.
     */
    public SymbolicLinkRule(ITmfStateSystemBuilder ssb,
            int quark,
            String [] attributePattern) {
        super(ssb, quark, Collections.singletonList(attributePattern));
    }

    @Override
    public ITmfStateValue getOngoingAggregatedState() {
        OptionalInt possibleQuark = getQuarkStream()
                .mapToInt(Integer::intValue)
                .findFirst();

        if (!possibleQuark.isPresent()) {
            /* Target quark does not exist at the moment */
            return TmfStateValue.nullValue();
        }

        try {
            int quark = possibleQuark.getAsInt();
            return getStateSystem().queryOngoingState(quark);
        } catch (AttributeNotFoundException e) {
            throw new IllegalStateException("Bad aggregation rule"); //$NON-NLS-1$
        }
    }

    @Override
    public ITmfStateInterval getAggregatedState(long timestamp) {
        ITmfStateSystem ss = getStateSystem();

        OptionalInt possibleQuark = getQuarkStream()
                .mapToInt(Integer::intValue)
                .findFirst();

        if (!possibleQuark.isPresent()) {
            /* Target quark does not exist at the moment */
            return new TmfStateInterval(ss.getStartTime(),
                    ss.getCurrentEndTime(),
                    getTargetQuark(),
                    TmfStateValue.nullValue());
        }

        try {
            int quark = possibleQuark.getAsInt();
            ITmfStateInterval otherInterval = getStateSystem().querySingleState(timestamp, quark);

            return new TmfStateInterval(otherInterval.getStartTime(),
                    otherInterval.getEndTime(),
                    getTargetQuark(),
                    otherInterval.getStateValue());

        } catch (AttributeNotFoundException | StateSystemDisposedException e) {
            throw new IllegalStateException("Bad aggregation rule"); //$NON-NLS-1$
        }
    }

}