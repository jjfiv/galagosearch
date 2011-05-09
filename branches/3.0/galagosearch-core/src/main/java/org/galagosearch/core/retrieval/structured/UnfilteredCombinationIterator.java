/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class UnfilteredCombinationIterator extends ScoreCombinationIterator {

  public UnfilteredCombinationIterator(Parameters parameters, ScoreValueIterator[] childIterators) {
    super(parameters, childIterators);
  }

  public int currentCandidate() {
    return MoveIterators.findMinimumDocument(iterators);
  }

  public boolean isDone() {
    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        return false;
      }
    }

    return true;
  }

  public boolean hasMatch(int identifier) {
    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone() && iterator.hasMatch(identifier)) {
        return true;
      }
    }
    return false;
  }

  public long totalEntries() {
    long max = 0;
    for (ValueIterator iterator : iterators) {
      max = Math.max(max, iterator.totalEntries());
    }
    return max;
  }

  /**
   * Moves all iterators at the current document to the next.
   *  *** BE VERY CAREFUL IN CALLING THIS FUNCTION ***
   *  - implemented as movePast(current)
   */
  public boolean next() throws IOException {
    int current = currentCandidate();
    movePast(current);
    int newcurrent = currentCandidate();
    moveTo(newcurrent); // necessary for conjunctions
    return (current != currentCandidate());
  }
}
