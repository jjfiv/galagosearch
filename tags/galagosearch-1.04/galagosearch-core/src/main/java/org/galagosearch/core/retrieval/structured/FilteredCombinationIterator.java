/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class FilteredCombinationIterator extends ScoreCombinationIterator {
    public FilteredCombinationIterator(Parameters parameters, ScoreIterator[] childIterators) {
        super(parameters, childIterators);
    }

    public int nextCandidate() {
        int candidate = 0;

        for (ScoreIterator iterator : iterators) {
            if (iterator.isDone()) {
                return Integer.MAX_VALUE;
            }
            candidate = Math.max(candidate, iterator.nextCandidate());
        }

        return candidate;
    }

    public boolean hasMatch(int document) {
        for (ScoreIterator iterator : iterators) {
            if (iterator.isDone() || !iterator.hasMatch(document)) {
                return false;
            }
        }

        return true;
    }

    public boolean isDone() {
        for (ScoreIterator iterator : iterators) {
            if (iterator.isDone()) {
                return true;
            }
        }

        return false;
    }
}
