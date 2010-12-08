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
    double[] weights;
    double weightSum;
    ScoreIterator[] iterators;
    boolean done;

    public ScoreCombinationIterator(Parameters parameters, ScoreIterator[] childIterators) {
      weights = new double[childIterators.length];
      weightSum = 0.0;
      for(int i = 0 ; i<weights.length ; i++){
        weights[i] = parameters.get(Integer.toString(i), 1.0);
        weightSum += weights[i];
      }
      this.iterators = childIterators;
    }

    public double score(int document, int length) {
        double total = 0;

        for(int i = 0; i < iterators.length ; i++){
            total += weights[i] * iterators[i].score(document, length);
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
