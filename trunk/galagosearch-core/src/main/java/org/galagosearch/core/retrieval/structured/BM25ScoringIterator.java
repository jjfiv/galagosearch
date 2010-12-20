/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.scoring.BM25Scorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author marc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount"})
public class BM25ScoringIterator extends ScoringFunctionIterator {
    public BM25ScoringIterator(Parameters p, DocumentOrderedCountIterator it)
        throws IOException {
        super( it, new BM25Scorer(p, it));
    }

    /**
     * Score is maxed by having count = length, but having both numbers be as high
     * as possible.
     * @return
     */
    public double maximumScore() {
        return function.score(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Minimized by having no occurrences.
     * @return
     */
    public double minimumScore() {
        return function.score(0, Integer.MAX_VALUE);
    }
}
