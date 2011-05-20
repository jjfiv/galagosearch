package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 * Implements the #all indicator operator.
 * @author irmarc
 */
public class UniversalIndicatorIterator extends AbstractIndicator {

  private int document;
  private boolean done;

  public UniversalIndicatorIterator(Parameters p, ValueIterator[] children) {
    super(p, children);
    // guarantees the correct order for the MoveIterators
    Arrays.sort(iterators, new Comparator<ValueIterator>() {

      public int compare(ValueIterator it1, ValueIterator it2) {
        return (int) (it1.totalEntries() - it2.totalEntries());
      }
    });

    document = MoveIterators.findMaximumDocument(iterators);
    done = (document == Integer.MAX_VALUE);
  }

  public int currentCandidate() {
    return document;
  }

  public void reset() throws IOException {
    for (ValueIterator i : iterators) {
      i.reset();
    }
    document = MoveIterators.findMaximumDocument(iterators);
    done = (document == Integer.MAX_VALUE);
  }

  public boolean getStatus() {
    return (context.document == this.document);
  }
  
  public boolean getStatus(int document) {
    return (document == this.document);
  }

  public boolean isDone() {
    return done;
  }

  public boolean moveTo(int identifier) throws IOException {
    for (ValueIterator iterator : iterators) {
      iterator.moveTo(identifier);
    }
    document = MoveIterators.findMaximumDocument(iterators);
    return (document == identifier);
  }

  public String getEntry() throws IOException {
    return Integer.toString(document);
  }

  /**
   * Uses the min of all lists. This is inaccurate, but taking the union is
   * a horrible idea.
   * @return
   */
  public long totalEntries() {
    long min = Integer.MAX_VALUE;
    for (ValueIterator iterator : iterators) {
      min = Math.min(min, iterator.totalEntries());
    }
    return min;
  }

  /**
   * Moves the child iterators on until they find a common document
   *  *** BE VERY CAREFUL IN CALLING THIS FUNCTION ***
   */
  public boolean next() throws IOException {
    iterators[0].movePast(document);
    document = MoveIterators.moveAllToSameDocument(iterators);
    done = (document == Integer.MAX_VALUE);
    return (!done);
  }
}
