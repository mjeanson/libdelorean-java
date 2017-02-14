/*
 * Copyright (C) 2017 EfficiOS Inc., Alexandre Montplaisir <alexmonthy@efficios.com>
 * Copyright (C) 2014-2016 École Polytechnique de Montréal
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.backend.historytree;

import java.nio.ByteBuffer;

/**
 * A Leaf node is a last-level node of a History Tree.
 *
 * A leaf node cannot have children, so it extends HTNode without adding
 * anything in particular.
 *
 * @author Florian Wininger
 */
final class LeafNode extends HTNode {

    /**
     * Initial constructor. Use this to initialize a new EMPTY node.
     *
     * @param config
     *            Configuration of the History Tree
     * @param seqNumber
     *            The (unique) sequence number assigned to this particular node
     * @param parentSeqNumber
     *            The sequence number of this node's parent node
     * @param start
     *            The earliest timestamp stored in this node
     */
    public LeafNode(HTConfig config, int seqNumber, int parentSeqNumber,
            long start) {
        super(config, seqNumber, parentSeqNumber, start);
    }

    @Override
    protected void readSpecificHeader(ByteBuffer buffer) {
        /* No specific header part */
    }

    @Override
    protected void writeSpecificHeader(ByteBuffer buffer) {
        /* No specific header part */
    }

    @Override
    public NodeType getNodeType() {
        return NodeType.LEAF;
    }

    @Override
    protected int getSpecificHeaderSize() {
        /* Empty */
        return 0;
    }

    @Override
    public String toStringSpecific() {
        /* Only used for debugging, shouldn't be externalized */
        return "Leaf Node"; //$NON-NLS-1$;
    }

}
