// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public abstract class ScoreCombinationIterator implements ScoreIterator {
    ScoreIterator[] iterators;
    boolean done;

    public ScoreCombinationIterator(Parameters parameters, ScoreIterator[] childIterators) {
        this.iterators = childIterators;
    }

    public double score(int document, int length) {
        float total = 0;

        for (ScoreIterator iterator : iterators) {
            total += iterator.score(document, length);
        }
        return total;
    }

    public void movePast(int document) throws IOException {
        for (ScoreIterator iterator : iterators) {
            iterator.movePast(document);
        }
    }

    public void moveTo(int document) throws IOException {
        for (ScoreIterator iterator : iterators) {
            iterator.moveTo(document);
        }
    }

    public void reset() throws IOException {
        for (ScoreIterator iterator : iterators) {
            iterator.reset();
        }
    }
}
