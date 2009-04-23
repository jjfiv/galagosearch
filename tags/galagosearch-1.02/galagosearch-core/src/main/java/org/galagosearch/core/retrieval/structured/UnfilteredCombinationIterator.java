/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import java.util.ArrayList;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class UnfilteredCombinationIterator extends ScoreCombinationIterator {
    public UnfilteredCombinationIterator(Parameters parameters, ScoreIterator[] childIterators) {
        super(parameters, childIterators);
    }

    public int nextCandidate() {
        int candidate = Integer.MAX_VALUE;

        for (ScoreIterator iterator : iterators) {
            if (iterator.isDone()) {
                continue;
            }
            candidate = Math.min(candidate, iterator.nextCandidate());
        }

        return candidate;
    }

    public boolean hasMatch(int document) {
        for (ScoreIterator iterator : iterators) {
            if (!iterator.isDone() && iterator.hasMatch(document)) {
                return true;
            }
        }

        return false;
    }
    
    public boolean isDone() {
        for (ScoreIterator iterator : iterators) {
            if (!iterator.isDone()) {
                return false;
            }
        }

        return true;
    }
}
