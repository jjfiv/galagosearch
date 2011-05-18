// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectProcedure;
import java.io.IOException;
import java.util.Arrays;
import org.galagosearch.core.index.ValueIterator;
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
public abstract class ScoreCombinationIterator implements ScoreValueIterator {

  protected double[] weights;
  protected double weightSum;
  protected ScoreValueIterator[] iterators;
  protected boolean done;
  protected boolean printing;

  public ScoreCombinationIterator(Parameters parameters,
          ScoreValueIterator[] childIterators) {

    weights = new double[childIterators.length];
    weightSum = 0.0;

    for (int i = 0; i < weights.length; i++) {
      String weightString = parameters.get(Integer.toString(i), "1.0");
      weights[i] = Double.parseDouble(weightString);
      weightSum += weights[i];
    }
    printing = parameters.get("print", false);
    this.iterators = childIterators;
  }

  public void setContext(DocumentContext context) {
    for (ScoreIterator iterator : iterators) {
      iterator.setContext(context);
    }
  }

  public DocumentContext getContext() {
    return iterators[0].getContext();
  }

  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Score combine nodes don't have singular values");
  }

  public int compareTo(ValueIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return currentCandidate() - other.currentCandidate();
  }

  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  public boolean moveTo(int identifier) throws IOException {
    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        iterator.moveTo(identifier);
      }
    }
    return hasMatch(identifier);
  }

  public double score() {
    double total = 0;

    for (int i = 0; i < iterators.length; i++) {
      double score = iterators[i].score();
      //if (printing) System.err.printf("it %s score= %f, weight = %f\n", iterators[i].toString(), score, weights[i]);
      total += weights[i] * score;
    }
    //if (printing) System.err.printf("Normalized score = %f\n", total / weightSum);
    return total / weightSum;
  }

  public double score(DocumentContext dc) {
    double total = 0;

    for (int i = 0; i < iterators.length; i++) {
      double score = iterators[i].score(dc);
      total += weights[i] * score;
    }
    return total / weightSum;
  }

  public boolean isDone() {
    return done;
  }

  public void reset() throws IOException {
    for (StructuredIterator iterator : iterators) {
      iterator.reset();
    }
  }

  public double minimumScore() {
    double min = 0;
    for (int i = 0; i < iterators.length; i++) {
      min += weights[i] * iterators[i].minimumScore();
    }
    return (min / weightSum);
  }

  public double maximumScore() {
    double max = 0;
    for (int i = 0; i < iterators.length; i++) {
      max += weights[i] * iterators[i].maximumScore();
    }
    return (max / weightSum);
  }
}
