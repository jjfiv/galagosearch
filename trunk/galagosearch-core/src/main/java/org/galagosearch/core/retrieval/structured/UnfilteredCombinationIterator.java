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
    public UnfilteredCombinationIterator(Parameters parameters, DocumentOrderedScoreIterator[] childIterators) {
        super(parameters, childIterators);
    }

    public int currentCandidate() {
        int candidate = Integer.MAX_VALUE;

        for (DocumentOrderedIterator iterator : iterators) {
            if (iterator.isDone()) {
                continue;
            }
            candidate = Math.min(candidate, iterator.currentCandidate());
        }

        return candidate;
    }

    public boolean hasMatch(int document) {
        for (DocumentOrderedIterator iterator : iterators) {
            if (!iterator.isDone() && iterator.hasMatch(document)) {
                return true;
            }
        }

        return false;
    }
    
    public boolean isDone() {
        for (DocumentOrderedIterator iterator : iterators) {
            if (!iterator.isDone()) {
                return false;
            }
        }

        return true;
    }
}
