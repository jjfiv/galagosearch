// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.tupleflow.Parameters;

/**
 * [sjh]: modified to scale the child nodes acording to weights in the parameters object
 * - this fixes hierarchical scaling problems by normalizing the node
 *
 * @author trevor, sjh
 */
public abstract class ScoreCombinationIterator implements ScoreIterator {
    Parameters parameters;
    ScoreIterator[] iterators;
    boolean done;

    public ScoreCombinationIterator(Parameters parameters, ScoreIterator[] childIterators) {
      this.parameters = parameters;
      this.iterators = childIterators;
    }

    public double score(int document, int length) {
        double total = 0;
        double weightSum = 0;

        for(int i = 0; i < iterators.length ; i++){
            double weight = parameters.get(Integer.toString(i), 1.0);
            total += weight * iterators[i].score(document, length);
            weightSum += weight;
        }
        return total / weightSum;
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
