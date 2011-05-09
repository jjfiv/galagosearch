// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class FilteredCombinationIterator extends ScoreCombinationIterator {
    public FilteredCombinationIterator(Parameters parameters, DocumentOrderedScoreIterator[] childIterators) {
        super(parameters, childIterators);
    }

    public int currentCandidate() {
        int candidate = 0;

        for (DocumentOrderedIterator iterator : iterators) {
            if (iterator.isDone()) {
                return Integer.MAX_VALUE;
            }
            candidate = Math.max(candidate, iterator.currentCandidate());
        }

        return candidate;
    }

    public boolean hasMatch(int document) {
        for (DocumentOrderedIterator iterator : iterators) {
            if (iterator.isDone() || !iterator.hasMatch(document)) {
                return false;
            }
        }

        return true;
    }

    public boolean isDone() {
        for (DocumentOrderedIterator iterator : iterators) {
            if (iterator.isDone()) {
                return true;
            }
        }

        return false;
    }
}
