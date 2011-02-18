/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.scoring.BM25RFScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"documentCount"})
public class BM25RFScoringIterator extends ScoringFunctionIterator {

    public BM25RFScoringIterator(Parameters p, DocumentOrderedCountIterator it)
            throws IOException {
        super(it, new BM25RFScorer(p, it));
    }

    /**
     * We override the score method here b/c the superclass version will always
     * call score, but with a 0 count, in case the scorer smoothes. In this case,
     * the count and length are irrelevant, and it's matching on the identifier
     * list that matters.
     *
     * @return
     */
    @Override
    public double score() {
        if (iterator.identifier() == documentToScore) {
            return function.score(iterator.count(), lengthOfDocumentToScore);
        } else {
            return 0;
        }
    }

    /**
     * For this particular scoring function, the parameters are irrelevant
     * @return
     */
    public double maximumScore() {
        return function.score(0, 0);
    }

    public double minimumScore() {
        return function.score(0, 0);
    }
}
