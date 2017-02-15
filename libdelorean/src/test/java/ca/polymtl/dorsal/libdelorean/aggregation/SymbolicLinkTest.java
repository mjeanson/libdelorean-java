/*
 * Copyright (C) 2016-2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.List;

import org.eclipse.jdt.annotation.NonNull;
import org.junit.Test;

import ca.polymtl.dorsal.libdelorean.ITmfStateSystemBuilder;
import ca.polymtl.dorsal.libdelorean.exceptions.AttributeNotFoundException;
import ca.polymtl.dorsal.libdelorean.exceptions.StateValueTypeException;
import ca.polymtl.dorsal.libdelorean.statevalue.ITmfStateValue;
import ca.polymtl.dorsal.libdelorean.statevalue.TmfStateValue;

/**
 * Tests for {@link SymbolicLinkRule}.
 *
 * @author Alexandre Montplaisir
 * @see IStateAggregationRule
 */
@SuppressWarnings("nls")
public class SymbolicLinkTest extends AggregationTestBase {

    @Override
    protected IStateAggregationRule createRuleWithParameters(ITmfStateSystemBuilder ssb,
            int targetQuark, List<String @NonNull []> patterns) {
        /* Will only work for tests that use one 'pattern'. */
        if (patterns.size() > 1) {
            throw new IllegalArgumentException();
        }
        return new SymbolicLinkRule(ssb, targetQuark, patterns.get(0));
    }

    @Override
    @Test
    public void testExistingAndNonExistingQuarks() {
        /* Does not apply to this rule */
    }

    /**
     * Build a history like this:
     *
     * <pre>
     * Time-------------- 1      2     3    4     5   6     7   8
     * attributeA
     * attributeB         [  1  ][  2  ]    [  1  ]   [  2  ]
     * </pre>
     *
     * Where attributeA will be a symlink to attributeB.
     */
    @Test
    public void fullTest() {
        ITmfStateSystemBuilder ss = getStateSystem();
        assertNotNull(ss);

        /* State values and attributes that will be used */
        ITmfStateValue VALUE_1 = TmfStateValue.newValueInt(1);
        ITmfStateValue VALUE_2 = TmfStateValue.newValueInt(2);
        ITmfStateValue NULL_VALUE = TmfStateValue.nullValue();

        int quarkAttributeA = ss.getQuarkAbsoluteAndAdd("attributeA");
        int quarkAttributeB = ss.getQuarkAbsoluteAndAdd("attributeB");

        /* Create and register the aggregation rule */
        IStateAggregationRule rule = new SymbolicLinkRule(ss,
                quarkAttributeA,
                new String[] { "attributeB" } );

        ss.addAggregationRule(rule);

        /* Populate the state system */
        try {
            assertEquals(NULL_VALUE, ss.queryOngoingState(quarkAttributeA));

            ss.modifyAttribute(10, VALUE_1, quarkAttributeB);
            assertEquals(VALUE_1, ss.queryOngoingState(quarkAttributeA));

            ss.modifyAttribute(20, VALUE_2, quarkAttributeB);
            assertEquals(VALUE_2, ss.queryOngoingState(quarkAttributeA));

            ss.modifyAttribute(30, NULL_VALUE, quarkAttributeB);
            assertEquals(NULL_VALUE, ss.queryOngoingState(quarkAttributeA));

            ss.modifyAttribute(40, VALUE_1, quarkAttributeB);
            assertEquals(VALUE_1, ss.queryOngoingState(quarkAttributeA));

            ss.modifyAttribute(50, NULL_VALUE, quarkAttributeB);
            assertEquals(NULL_VALUE, ss.queryOngoingState(quarkAttributeA));

            ss.modifyAttribute(60, VALUE_2, quarkAttributeB);
            assertEquals(VALUE_2, ss.queryOngoingState(quarkAttributeA));

            ss.modifyAttribute(70, NULL_VALUE, quarkAttributeB);
            assertEquals(NULL_VALUE, ss.queryOngoingState(quarkAttributeA));

        } catch (StateValueTypeException | AttributeNotFoundException e) {
            fail(e.getMessage());
        }

        ss.closeHistory(80);

        /* Check the results of queries */
        verifyInterval(  5, quarkAttributeA,   0,   9, NULL_VALUE);
        verifyInterval( 15, quarkAttributeA,  10,  19, VALUE_1);
        verifyInterval( 25, quarkAttributeA,  20,  29, VALUE_2);
        verifyInterval( 35, quarkAttributeA,  30,  39, NULL_VALUE);
        verifyInterval( 45, quarkAttributeA,  40,  49, VALUE_1);
        verifyInterval( 55, quarkAttributeA,  50,  59, NULL_VALUE);
        verifyInterval( 65, quarkAttributeA,  60,  69, VALUE_2);
        verifyInterval( 75, quarkAttributeA,  70,  80, NULL_VALUE);
    }

}