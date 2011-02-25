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
public class UnfilteredCombinationIterator extends ScoreCombinationIterator {
    public UnfilteredCombinationIterator(Parameters parameters, ScoreIterator[] childIterators) {
        super(parameters, childIterators);
    }

    public int currentIdentifier() {
        int candidate = Integer.MAX_VALUE;

        for (ScoreIterator iterator : iterators) {
            if (iterator.isDone()) {
                continue;
            }
            candidate = Math.min(candidate, iterator.currentIdentifier());
        }

        return candidate;
    }
    
    public boolean isDone() {
        for (StructuredIterator iterator : iterators) {
            if (!iterator.isDone()) {
                return false;
            }
        }

        return true;
    }
}
