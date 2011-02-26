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

  public boolean next() throws IOException {
    boolean moved = false;
    int current = currentIdentifier();
    for (ScoreValueIterator iterator : iterators) {
      if (!iterator.isDone() && iterator.currentIdentifier() == current) {
        iterator.next();
        moved = true;
      }
    }
    return moved;
  }

  public int currentIdentifier() {
    int candidate = Integer.MAX_VALUE;

    for (ScoreValueIterator iterator : iterators) {
      if (iterator.isDone()) {
        continue;
      }
      candidate = Math.min(candidate, iterator.currentIdentifier());
    }

    return candidate;
  }

  public boolean isDone() {
    for (StructuredIterator iterator : iterators) {
      if (!iterator.isDone()) {
        return false;
      }
    }

    return true;
  }

  public boolean hasMatch(int identifier) {
    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone() && iterator.currentIdentifier() == identifier) {
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
}
