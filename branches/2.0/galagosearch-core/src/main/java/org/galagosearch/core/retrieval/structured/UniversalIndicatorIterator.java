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
public class UniversalIndicatorIterator extends IndicatorIterator {

  public UniversalIndicatorIterator(Parameters p, ValueIterator[] children) {
    super(p, children);
    // guarantees the correct order for the MoveIterators
    Arrays.sort(iterators, new Comparator<ValueIterator>() {

      public int compare(ValueIterator it1, ValueIterator it2) {
        return (int) (it1.totalEntries() - it2.totalEntries());
      }
    });
    try {
      document = MoveIterators.moveAllToSameDocument(iterators);
      done = (document == Integer.MAX_VALUE);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    document = MoveIterators.moveAllToSameDocument(iterators);
    done = (document == Integer.MAX_VALUE);
  }

  @Override
  public boolean getStatus() {
    return (context.document == this.document);
  }

  /**
   * Throws all iterators in a heap, moves the lowest one, then moves them all to the
   * same location.
   * @return
   * @throws IOException
   */
  public boolean next() throws IOException {
    PriorityQueue<ValueIterator> heap = new PriorityQueue<ValueIterator>();
    heap.addAll(Arrays.asList(iterators));
    heap.peek().next();
    document = MoveIterators.moveAllToSameDocument(iterators);
    done = (document == Integer.MAX_VALUE);
    return (!done);
  }

  public boolean moveTo(int identifier) throws IOException {
    boolean success = false;
    for (ValueIterator iterator : iterators) {
      iterator.moveTo(identifier);
    }
    document = MoveIterators.moveAllToSameDocument(iterators);
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
}
