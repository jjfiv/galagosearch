/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class UnfilteredCombinationIterator extends ScoreCombinationIterator {
    public UnfilteredCombinationIterator(Parameters parameters, ScoreIterator[] childIterators) {
        super(parameters, childIterators);
    }

    public int identifier() {
        int candidate = Integer.MAX_VALUE;

        for (ValueIterator iterator : iterators) {
            if (iterator.isDone()) {
                continue;
            }
            candidate = Math.min(candidate, iterator.currentCandidate());
        }

        return candidate;
    }

    public boolean hasMatch(int document) {
        for (ValueIterator iterator : iterators) {
            if (!iterator.isDone() && iterator.hasMatch(document)) {
                return true;
            }
        }

        return false;
    }
    
    public boolean isDone() {
        for (ValueIterator iterator : iterators) {
            if (!iterator.isDone()) {
                return false;
            }
        }

        return true;
    }
}
