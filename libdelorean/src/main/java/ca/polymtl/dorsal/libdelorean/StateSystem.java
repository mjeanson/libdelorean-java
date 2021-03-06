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

package ca.polymtl.dorsal.libdelorean;

import ca.polymtl.dorsal.libdelorean.aggregation.IStateAggregationRule;
import ca.polymtl.dorsal.libdelorean.backend.IStateHistoryBackend;
import ca.polymtl.dorsal.libdelorean.exceptions.AttributeNotFoundException;
import ca.polymtl.dorsal.libdelorean.exceptions.StateSystemDisposedException;
import ca.polymtl.dorsal.libdelorean.exceptions.TimeRangeException;
import ca.polymtl.dorsal.libdelorean.interval.StateInterval;
import ca.polymtl.dorsal.libdelorean.statevalue.IntegerStateValue;
import ca.polymtl.dorsal.libdelorean.statevalue.StateValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * This is the core class of the Generic State System. It contains all the
 * methods to build and query a state history. It's exposed externally through
 * the IStateSystemQuerier and IStateSystemBuilder interfaces, depending if the
 * user needs read-only access or read-write access.
 *
 * When building, DON'T FORGET to call .closeHistory() when you are done
 * inserting intervals, or the storage backend will have no way of knowing it
 * can close and write itself to disk, and its thread will keep running.
 *
 * @author alexmont
 *
 */
class StateSystem implements IStateSystemWriter {

    private static final Logger LOGGER = Logger.getLogger(StateSystem.class.getName());

    /* References to the inner structures */
    private final AttributeTree attributeTree;
    private final TransientState transState;
    private final IStateHistoryBackend backend;

    /**
     * Map to store the state aggregation rules. The key the quark, effectively
     * limiting each quark to one rule.
     */
    private final @NotNull Map<Integer, IStateAggregationRule> aggregationRules = new ConcurrentHashMap<>();

    /* Latch tracking if the state history is done building or not */
    private final CountDownLatch finishedLatch = new CountDownLatch(1);

    private boolean isDisposed = false;

    /**
     * New-file constructor. For when you build a state system with a new file,
     * or if the back-end does not require a file on disk.
     *
     * @param backend
     *            Back-end plugin to use
     */
    public StateSystem(@NotNull IStateHistoryBackend backend) {
        this.backend = backend;
        this.transState = new TransientState(backend);
        this.attributeTree = new AttributeTree(this);
    }

    /**
     * General constructor
     *
     * @param backend
     *            The "state history storage" back-end to use.
     * @param newFile
     *            Put true if this is a new history started from scratch. It is
     *            used to tell the state system where to get its attribute tree.
     * @throws IOException
     *             If there was a problem creating the new history file
     */
    public StateSystem(@NotNull IStateHistoryBackend backend, boolean newFile)
            throws IOException {
        this.backend = backend;
        this.transState = new TransientState(backend);

        if (newFile) {
            attributeTree = new AttributeTree(this);
        } else {
            /* We're opening an existing file */
            this.attributeTree = new AttributeTree(this, backend.supplyAttributeTreeReader());
            transState.setInactive();
            finishedLatch.countDown(); /* The history is already built */
        }
    }

    @Override
    public String getSSID() {
        return backend.getSSID();
    }

    @Override
    public void waitUntilBuilt() {
        try {
            finishedLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean waitUntilBuilt(long timeout) {
        boolean ret = false;
        try {
            ret = finishedLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public synchronized void dispose() {
        isDisposed = true;
        if (transState.isActive()) {
            transState.setInactive();
        }
        backend.dispose();
    }

    //--------------------------------------------------------------------------
    //        General methods related to the attribute tree
    //--------------------------------------------------------------------------

    /**
     * Get the attribute tree associated with this state system. This should be
     * the only way of accessing it (and if subclasses want to point to a
     * different attribute tree than their own, they should only need to
     * override this).
     *
     * @return The attribute tree
     */
    public AttributeTree getAttributeTree() {
        return attributeTree;
    }

    /**
     * Method used by the attribute tree when creating new attributes, to keep
     * the attribute count in the transient state in sync.
     */
    public void addEmptyAttribute() {
        transState.addEmptyEntry();
    }

    @Override
    public int getNbAttributes() {
        return getAttributeTree().getNbAttributes();
    }

    @Override
    public String getAttributeName(int attributeQuark) {
        return getAttributeTree().getAttributeName(attributeQuark);
    }

    @Override
    public String getFullAttributePath(int attributeQuark) {
        return getAttributeTree().getFullAttributeName(attributeQuark);
    }

    @Override
    public String[] getFullAttributePathArray(int attributeQuark) {
        return getAttributeTree().getFullAttributePathArray(attributeQuark);
    }

    //--------------------------------------------------------------------------
    //        Methods related to the storage backend
    //--------------------------------------------------------------------------

    @Override
    public long getStartTime() {
        return backend.getStartTime();
    }

    @Override
    public long getCurrentEndTime() {
        return backend.getEndTime();
    }

    @Override
    public void closeHistory(long endTime) throws TimeRangeException {
        File attributeTreeFile;
        long attributeTreeFilePos;
        long realEndTime = endTime;

        if (realEndTime < backend.getEndTime()) {
            /*
             * This can happen (empty nodes pushing the border further, etc.)
             * but shouldn't be too big of a deal.
             */
            realEndTime = backend.getEndTime();
        }
        transState.closeTransientState(realEndTime);
        backend.finishBuilding(realEndTime);

        attributeTreeFile = backend.supplyAttributeTreeWriterFile();
        attributeTreeFilePos = backend.supplyAttributeTreeWriterFilePosition();
        if (attributeTreeFile != null) {
            /*
             * If null was returned, we simply won't save the attribute tree,
             * too bad!
             */
            getAttributeTree().writeSelf(attributeTreeFile, attributeTreeFilePos);
        }
        finishedLatch.countDown(); /* Mark the history as finished building */
    }

    //--------------------------------------------------------------------------
    //        Quark-retrieving methods
    //--------------------------------------------------------------------------

    @Override
    public int getQuarkAbsolute(String... attribute)
            throws AttributeNotFoundException {
        return getAttributeTree().getQuarkDontAdd(-1, attribute);
    }

    @Override
    public int getQuarkAbsoluteAndAdd(String... attribute) {
        return getAttributeTree().getQuarkAndAdd(-1, attribute);
    }

    @Override
    public int getQuarkRelative(int startingNodeQuark, String... subPath)
            throws AttributeNotFoundException {
        return getAttributeTree().getQuarkDontAdd(startingNodeQuark, subPath);
    }

    @Override
    public int getQuarkRelativeAndAdd(int startingNodeQuark, String... subPath) {
        return getAttributeTree().getQuarkAndAdd(startingNodeQuark, subPath);
    }

    @Override
    public List<Integer> getSubAttributes(int quark, boolean recursive)
            throws AttributeNotFoundException {
        return getAttributeTree().getSubAttributes(quark, recursive);
    }

    @Override
    public List<Integer> getSubAttributes(int quark, boolean recursive, String pattern)
            throws AttributeNotFoundException {
        List<Integer> all = getSubAttributes(quark, recursive);
        List<Integer> ret = new LinkedList<>();
        for (Integer attQuark : all) {
            String name = getAttributeName(attQuark.intValue());
            if (name.matches(pattern)) {
                ret.add(attQuark);
            }
        }
        return ret;
    }

    @Override
    public int getParentAttributeQuark(int quark) {
        return getAttributeTree().getParentAttributeQuark(quark);
    }

    @Override
    public List<Integer> getQuarks(String... pattern) {
        List<Integer> quarks = new LinkedList<>();
        List<String> prefix = new LinkedList<>();
        List<String> suffix = new LinkedList<>();
        boolean split = false;
        String[] prefixStr;
        String[] suffixStr;
        List<Integer> directChildren;
        int startingAttribute;

        /* Fill the "prefix" and "suffix" parts of the pattern around the '*' */
        for (String entry : pattern) {
            if (entry.equals("*")) { //$NON-NLS-1$
                if (split) {
                    /*
                     * Split was already true? This means there was more than
                     * one wildcard. This is not supported, return an empty
                     * list.
                     */
                    return quarks;
                }
                split = true;
                continue;
            }

            if (split) {
                suffix.add(entry);
            } else {
                prefix.add(entry);
            }
        }
        prefixStr = prefix.toArray(new String[prefix.size()]);
        suffixStr = suffix.toArray(new String[suffix.size()]);

        /*
         * If there was no wildcard, we'll only return the one matching
         * attribute, if there is one.
         */
        if (!split) {
            int quark;
            try {
                quark = getQuarkAbsolute(prefixStr);
            } catch (AttributeNotFoundException e) {
                /* It's fine, we'll just return the empty List */
                return quarks;
            }
            quarks.add(quark);
            return quarks;
        }

        try {
            if (prefix.size() == 0) {
                /*
                 * If 'prefix' is empty, this means the wildcard was the first
                 * element. Look for the root node's sub-attributes.
                 */
                startingAttribute = -1;
            } else {
                startingAttribute = getQuarkAbsolute(prefixStr);
            }
            directChildren = getSubAttributes(startingAttribute, false);
        } catch (AttributeNotFoundException e) {
            /* That attribute path did not exist, return the empty array */
            return quarks;
        }

        /*
         * Iterate of all the sub-attributes, and only keep those who match the
         * 'suffix' part of the initial pattern.
         */
        for (int childQuark : directChildren) {
            int matchingQuark;
            try {
                matchingQuark = getQuarkRelative(childQuark, suffixStr);
            } catch (AttributeNotFoundException e) {
                continue;
            }
            quarks.add(matchingQuark);
        }

        return quarks;
    }

    //--------------------------------------------------------------------------
    //        Methods related to insertions in the history
    //--------------------------------------------------------------------------

    @Override
    public void modifyAttribute(long t, StateValue value, int attributeQuark)
            throws TimeRangeException, AttributeNotFoundException {
        if (value == null) {
            /*
             * TODO Replace with @NonNull parameter (will require fixing all the
             * state providers!)
             */
            throw new IllegalArgumentException();
        }
        transState.processStateChange(t, value, attributeQuark);
    }

    @Override
    public void incrementAttribute(long t, int attributeQuark)
            throws TimeRangeException,
            AttributeNotFoundException {
        StateValue stateValue = queryOngoingState(attributeQuark);
        int prevValue = 0;
        /* if the attribute was previously null, start counting at 0 */
        if (!stateValue.isNull()) {
            prevValue = ((IntegerStateValue) stateValue).getValue();
        }
        modifyAttribute(t, StateValue.newValueInt(prevValue + 1), attributeQuark);
    }

    @Override
    public void pushAttribute(long t, StateValue value, int attributeQuark)
            throws TimeRangeException, AttributeNotFoundException {
        int stackDepth;
        int subAttributeQuark;
        StateValue previousSV = transState.getOngoingStateValue(attributeQuark);

        if (previousSV.isNull()) {
            /*
             * If the StateValue was null, this means this is the first time we
             * use this attribute. Leave stackDepth at 0.
             */
            stackDepth = 0;
        } else if (previousSV instanceof IntegerStateValue) {
            /* Previous value was an integer, all is good, use it */
            stackDepth = ((IntegerStateValue) previousSV).getValue();
        } else {
            /* Previous state of this attribute was another type? Not good! */
            throw new IllegalArgumentException(getSSID() + " Quark:" + attributeQuark + ", Type:" + previousSV.getClass().getSimpleName() + ", Expected:" + IntegerStateValue.class.getSimpleName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }

        if (stackDepth >= 100000) {
            /*
             * Limit stackDepth to 100000, to avoid having Attribute Trees grow
             * out of control due to buggy insertions
             */
            String message = " Stack limit reached, not pushing"; //$NON-NLS-1$
            throw new AttributeNotFoundException(getSSID() + " Quark:" + attributeQuark + message); //$NON-NLS-1$
        }

        stackDepth++;
        subAttributeQuark = getQuarkRelativeAndAdd(attributeQuark, String.valueOf(stackDepth));

        modifyAttribute(t, StateValue.newValueInt(stackDepth), attributeQuark);
        modifyAttribute(t, value, subAttributeQuark);
    }

    @Override
    public StateValue popAttribute(long t, int attributeQuark)
            throws AttributeNotFoundException, TimeRangeException {
        /* These are the state values of the stack-attribute itself */
        StateValue previousSV = transState.getOngoingStateValue(attributeQuark);

        if (previousSV.isNull()) {
            /*
             * Trying to pop an empty stack. This often happens at the start of
             * traces, for example when we see a syscall_exit, without having
             * the corresponding syscall_entry in the trace. Just ignore
             * silently.
             */
            return null;
        }
        if (!(previousSV instanceof IntegerStateValue)) {
            /*
             * The existing value was not an integer (which is expected for
             * stack tops), this doesn't look like a valid stack attribute.
             */
            throw new IllegalArgumentException(getSSID() + " Quark:" + attributeQuark + ", Type:" + previousSV.getClass().getSimpleName() + ", Expected:" + IntegerStateValue.class.getSimpleName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }

        int stackDepth = ((IntegerStateValue) previousSV).getValue();

        if (stackDepth <= 0) {
            /* This on the other hand should not happen... */
            throw new IllegalArgumentException(getSSID() + " Quark:" + attributeQuark + ", Stack depth:" + stackDepth);  //$NON-NLS-1$//$NON-NLS-2$
        }

        /* The attribute should already exist at this point */
        int subAttributeQuark = getQuarkRelative(attributeQuark, String.valueOf(stackDepth));
        StateValue poppedValue = queryOngoingState(subAttributeQuark);

        /* Update the state value of the stack-attribute */
        StateValue nextSV;
        if (--stackDepth == 0) {
            /* Store a null state value */
            nextSV = StateValue.nullValue();
        } else {
            nextSV = StateValue.newValueInt(stackDepth);
        }
        modifyAttribute(t, nextSV, attributeQuark);

        /* Delete the sub-attribute that contained the user's state value */
        removeAttribute(t, subAttributeQuark);

        return poppedValue;
    }

    @Override
    public void removeAttribute(long t, int attributeQuark)
            throws TimeRangeException, AttributeNotFoundException {
        if (attributeQuark < 0) {
            throw new IllegalArgumentException();
        }

        /*
         * Nullify our children first, recursively. We pass 'false' because we
         * handle the recursion ourselves.
         */
        List<Integer> childAttributes = getSubAttributes(attributeQuark, false);
        for (int childNodeQuark : childAttributes) {
            if (attributeQuark == childNodeQuark) {
                /* Something went very wrong when building out attribute tree */
                throw new IllegalStateException();
            }
            removeAttribute(t, childNodeQuark);
        }
        /* Nullify ourselves */
        transState.processStateChange(t, StateValue.nullValue(), attributeQuark);
    }

    //--------------------------------------------------------------------------
    //        "Current" query/update methods
    //--------------------------------------------------------------------------

    @Override
    public StateValue queryOngoingState(int attributeQuark) throws AttributeNotFoundException {
        /* Check if the attribute is an aggregate */
        StateValue aggregatedValue = getOngoingAggregatedState(attributeQuark);
        if (aggregatedValue != null) {
            return aggregatedValue;
        }
        return transState.getOngoingStateValue(attributeQuark);
    }

    @Override
    public long getOngoingStartTime(int attribute) throws AttributeNotFoundException {
        if (aggregationRules.containsKey(attribute)) {
            throw new IllegalArgumentException("Cannot get the ongoing start time of aggregate attributes " + //$NON-NLS-1$
                    "(Attribute #" + attribute); //$NON-NLS-1$
        }
        return transState.getOngoingStartTime(attribute);
    }

    @Override
    public void updateOngoingState(StateValue newValue, int attributeQuark)
            throws AttributeNotFoundException {
        transState.changeOngoingStateValue(attributeQuark, newValue);
    }

    /**
     * Modify the whole "ongoing state" (state values + start times). This can
     * be used when "seeking" a state system to a different point in the trace
     * (and restoring the known stateInfo at this location). Use with care!
     *
     * @param newStateIntervals
     *            The new List of state values to use as ongoing state info
     */
    protected void replaceOngoingState(@NotNull List<StateInterval> newStateIntervals) {
        transState.replaceOngoingState(newStateIntervals);
    }

    //--------------------------------------------------------------------------
    //        Regular query methods (sent to the back-end)
    //--------------------------------------------------------------------------

    @Override
    public synchronized List<StateInterval> queryFullState(long t)
            throws TimeRangeException, StateSystemDisposedException {
        if (isDisposed) {
            throw new StateSystemDisposedException();
        }

        final int nbAttr = getNbAttributes();
        List<StateInterval> stateInfo = new ArrayList<>(nbAttr);

        /* Bring the size of the array to the current number of attributes */
        for (int i = 0; i < nbAttr; i++) {
            stateInfo.add(null);
        }

        /*
         * If we are currently building the history, also query the "ongoing"
         * states for stuff that might not yet be written to the history.
         */
        if (transState.isActive()) {
            transState.doQuery(stateInfo, t);
        }

        /* Query the storage backend */
        backend.doQuery(stateInfo, t);

        /*
         * Replace the values of aggregated attributes with their correct ones.
         * TODO Find a way to do this in one pass?
         */
        aggregationRules.values().forEach(rule -> {
            int quark = rule.getTargetQuark();
            StateInterval newValue = rule.getAggregatedState(t);
            stateInfo.set(quark, newValue);
        });

        /*
         * We should have previously inserted an interval for every attribute.
         */
        for (StateInterval interval : stateInfo) {
            if (interval == null) {
                throw new IllegalStateException("Incoherent interval storage"); //$NON-NLS-1$
            }
        }
        return stateInfo;
    }

    @Override
    public StateInterval querySingleState(long t, int attributeQuark)
            throws AttributeNotFoundException, TimeRangeException, StateSystemDisposedException {
        if (isDisposed) {
            throw new StateSystemDisposedException();
        }

        /* First check if the target quark is an aggregate */
        StateInterval ret = getAggregatedState(attributeQuark, t);
        if (ret != null) {
            return ret;
        }

        ret = transState.getIntervalAt(t, attributeQuark);
        if (ret == null) {
            /*
             * The transient state did not have the information, let's look into
             * the backend next.
             */
            ret = backend.doSingularQuery(t, attributeQuark);
        }

        if (ret == null) {
            /*
             * If we did our job correctly, there should be intervals for every
             * possible attribute, over all the valid time range.
             */
            throw new IllegalStateException("Incoherent interval storage"); //$NON-NLS-1$
        }
        return ret;
    }

    @Override
    public Map<Integer, StateInterval> queryStates(long t, Set<Integer> quarks) {
        if (isDisposed) {
            throw new StateSystemDisposedException();
        }

        Map<Integer, StateInterval> results = new HashMap<>(quarks.size());
        Set<Integer> remainingQuarks = new HashSet<>(quarks);

        /*
         * First filter out the quarks we can manage at the core state system
         * level. This includes aggregate attributes, and quarks for which the
         * state is part of the ongoing state.
         */
        Iterator<Integer> iter = remainingQuarks.iterator();
        while (iter.hasNext()) {
            int quark = iter.next();
            /* Check if it's an aggregate. */
            StateInterval interval = getAggregatedState(quark, t);
            if (interval == null) {
                /* Check if it's in the transient state. */
                interval = transState.getIntervalAt(t, quark);
            }
            if (interval != null) {
                results.put(quark, interval);
                iter.remove();
            }
        }

        /* Remaining quarks will be sent to the backend. */
        backend.doPartialQuery(t, remainingQuarks, results);

        return results;
    }

    // --------------------------------------------------------------------------
    // State aggregation methods
    // --------------------------------------------------------------------------

    @Override
    public void addAggregationRule(@NotNull IStateAggregationRule rule) {
        if (rule.getStateSystem() != this) {
            throw new IllegalArgumentException();
        }
        aggregationRules.put(Integer.valueOf(rule.getTargetQuark()), rule);
    }

    private @Nullable StateValue getOngoingAggregatedState(int quark) {
        IStateAggregationRule rule = aggregationRules.get(Integer.valueOf(quark));
        if (rule == null) {
            return null;
        }
        return rule.getOngoingAggregatedState();
    }

    private @Nullable StateInterval getAggregatedState(int quark, long timestamp) {
        IStateAggregationRule rule = aggregationRules.get(Integer.valueOf(quark));
        if (rule == null) {
            return null;
        }
        return rule.getAggregatedState(timestamp);
    }

    //--------------------------------------------------------------------------
    //        Debug methods
    //--------------------------------------------------------------------------

    static void logMissingInterval(int attribute, long timestamp) {
        LOGGER.severe("No data found in history for attribute " + //$NON-NLS-1$
                attribute + " at time " + timestamp + //$NON-NLS-1$
                ", returning dummy interval"); //$NON-NLS-1$
    }

}
