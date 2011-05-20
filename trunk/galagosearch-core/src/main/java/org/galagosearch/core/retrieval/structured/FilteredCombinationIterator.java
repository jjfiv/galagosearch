// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *  -- DEPRECATED -- (sjh)
 * 
 * Use the FilterIterator instead:
 *  - #filter( #all(#query) (#query) )
 * 
 * @author trevor
 */
public class FilteredCombinationIterator extends ScoreCombinationIterator {

  public FilteredCombinationIterator(Parameters parameters, ScoreValueIterator[] childIterators) {
    super(parameters, childIterators);
  }

  public int currentCandidate() {
    return MoveIterators.findMaximumDocument(iterators);
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

  public long totalEntries() {
    long max = 0;
    for (ValueIterator iterator : iterators) {
      max = Math.max(max, iterator.totalEntries());
    }
    return max;
  }

  /**
   *  *** BE VERY CAREFUL IN CALLING THIS FUNCTION ***
   */
  public boolean next() throws IOException {
    MoveIterators.moveAllToSameDocument(iterators);
    return (! isDone());
  }
}
