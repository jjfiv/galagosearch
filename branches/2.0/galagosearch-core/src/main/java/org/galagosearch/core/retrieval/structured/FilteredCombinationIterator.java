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
public class FilteredCombinationIterator extends ScoreCombinationIterator {

  public FilteredCombinationIterator(Parameters parameters, ScoreValueIterator[] childIterators) {
    super(parameters, childIterators);
  }

  public int currentIdentifier() {
    int candidate = 0;

    for (ValueIterator iterator : iterators) {
      if (iterator.isDone()) {
        return Integer.MAX_VALUE;
      }
      candidate = Math.max(candidate, iterator.currentIdentifier());
    }

    return candidate;
  }

  public boolean hasMatch(int document) {
    for (ValueIterator iterator : iterators) {
      if (iterator.isDone() || !iterator.hasMatch(document)) {
        return false;
      }
    }

    return true;
  }

  public boolean isDone() {
    for (ValueIterator iterator : iterators) {
      if (iterator.isDone()) {
        return true;
      }
    }
    return false;
  }

  public boolean next() throws IOException {
    return (moveTo(currentIdentifier()+1));
  }

  public long totalEntries() {
    long max = 0;
    for (ValueIterator iterator : iterators) {
      max = Math.max(max, iterator.totalEntries());
    }
    return max;
  }
}
