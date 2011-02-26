/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class FilteredCombinationIterator extends ScoreCombinationIterator {

  private class IteratorComparator implements Comparator<ScoreValueIterator> {
    public int compare(ScoreValueIterator a, ScoreValueIterator b) {
      return (a.currentIdentifier() - b.currentIdentifier());
    }
  }

  Comparator<ScoreValueIterator> comparator;

  public FilteredCombinationIterator(Parameters parameters, ScoreValueIterator[] childIterators) {
    super(parameters, childIterators);
    comparator = new IteratorComparator();
  }

  public boolean next() throws IOException {
    if (!isDone()) {
      while (!allMatch() && !done) {
        iterators[0].next();
        Arrays.sort(iterators, comparator);
      }
    }
    return (isDone() == false);
  }

  private boolean allMatch() {
    int current = iterators[0].currentIdentifier();
    for (int i = 0; i < iterators.length; i++) {
      if (iterators[i].currentIdentifier() != current)
        return false;
    }
    return true;
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
  
  public boolean isDone() {
    for (ScoreIterator iterator : iterators) {
      if (iterator.isDone()) {
        return true;
      }
    }
    return false;
  }

  public long totalEntries() {
    long min = Long.MAX_VALUE;
    for (ValueIterator iterator : iterators) {
      min = Math.min(min, iterator.totalEntries());
    }
    return min;
  }
}
