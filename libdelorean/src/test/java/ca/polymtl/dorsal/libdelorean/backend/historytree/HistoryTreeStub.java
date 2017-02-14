/*
 * Copyright (C) 2015 École Polytechnique de Montréal
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package ca.polymtl.dorsal.libdelorean.backend.historytree;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.eclipse.jdt.annotation.NonNullByDefault;

import com.google.common.collect.Iterables;

/**
 * Stub class to unit test the history tree. You can set the size of the
 * interval section before using the tree, in order to fine-tune the test.
 *
 * Note to developers: This tree is not meant to be used with a backend. It just
 * exposes some info from the history tree.
 *
 * @author Geneviève Bastien
 */
@NonNullByDefault
public class HistoryTreeStub extends HistoryTree {

    /**
     * Constructor for this history tree stub
     *
     * @param newStateFile
     *            The name of the history file
     * @param blockSize
     *            The size of each "block" on disk. One node will always fit in
     *            one block.
     * @param maxChildren
     *            The maximum number of children allowed per core (non-leaf)
     *            node.
     * @param providerVersion
     *            The version of the state provider. If a file already exists,
     *            and their versions match, the history file will not be rebuilt
     *            uselessly.
     * @param startTime
     *            The start time of the history
     * @throws IOException
     *             If an error happens trying to open/write to the file
     *             specified in the config
     */
    public HistoryTreeStub(File newStateFile,
            int blockSize,
            int maxChildren,
            int providerVersion,
            long startTime) throws IOException {
        super(newStateFile,
                blockSize,
                maxChildren,
                providerVersion,
                startTime);
    }

    @Override
    public List<HTNode> getLatestBranch() {
        return requireNonNull(super.getLatestBranch());
    }

    /**
     * Get the latest leaf of the tree
     *
     * @return The current leaf node of the tree
     */
    public HTNode getLatestLeaf() {
        List<HTNode> latest = getLatestBranch();
        return requireNonNull(Iterables.getLast(latest));
    }

    /**
     * Get the node from the latest branch at a given position, 0 being the root
     * and <size of latest branch - 1> being a leaf node.
     *
     * @param pos
     *            The position at which to return the node
     * @return The node at position pos
     */
    public HTNode getNodeAt(int pos) {
        List<HTNode> latest = getLatestBranch();
        return requireNonNull(latest.get(pos));
    }

    /**
     * Get the depth of the tree
     *
     * @return The depth of the tree
     */
    public int getDepth() {
        return getLatestBranch().size();
    }

}
