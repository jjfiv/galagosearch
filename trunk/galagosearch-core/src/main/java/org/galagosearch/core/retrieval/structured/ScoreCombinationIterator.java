// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.tupleflow.Parameters;

/**
 * [sjh]: modified to scale the child nodes acording to weights in the parameters object
 * - this fixes hierarchical scaling problems by normalizing the node
 *
 * [irmarc]: Part of a refactor - this node now represents a score iterator that navigates
 *          via document-ordered methods
 *
 * @author trevor, sjh, irmarc
 */
public abstract class ScoreCombinationIterator extends DocumentOrderedScoreIterator {
    double[] weights;
    double weightSum;
    DocumentOrderedScoreIterator[] iterators;
    boolean done;

    public ScoreCombinationIterator(Parameters parameters, 
            DocumentOrderedScoreIterator[] childIterators) {
      weights = new double[childIterators.length];
      weightSum = 0.0;
      for(int i = 0 ; i<weights.length ; i++){
        weights[i] = parameters.get(Integer.toString(i), 1.0);
        weightSum += weights[i];
      }
      this.iterators = childIterators;
    }

    public double score() {
        double total = 0;

        for(int i = 0; i < iterators.length ; i++){
            total += weights[i] * iterators[i].score();
        }
        return total / weightSum;
    }

    public void movePast(int document) throws IOException {
        for (DocumentOrderedIterator iterator : iterators) {
            iterator.movePast(document);
        }
    }

    public void moveTo(int document) throws IOException {
        for (DocumentOrderedIterator iterator : iterators) {
            iterator.moveTo(document);
        }
    }

    public boolean skipToDocument(int document) throws IOException {
        boolean skipped = true;
        for (DocumentOrderedIterator iterator : iterators) {
            skipped = skipped && iterator.skipToDocument(document);
        }
        return skipped;
    }

    public void reset() throws IOException {
        for (DocumentOrderedIterator iterator : iterators) {
            iterator.reset();
        }
    }
}
