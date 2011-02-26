// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.PriorityQueue;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.util.ExtentArray;

/**
 *
 * @author trevor
 */
public abstract class ExtentConjunctionIterator extends ExtentCombinationIterator {

  public ExtentConjunctionIterator(ExtentValueIterator[] extIterators) {
    this.done = false;
    iterators = new PriorityQueue<ExtentValueIterator>(extIterators.length);
    for (ExtentValueIterator ei : extIterators) {
      iterators.add(ei);
    }
    this.extents = new ExtentArray();
  }

  // Move the lowest one forward first, then keep moving them forward
  // until they all match on the id.
  public boolean next() throws IOException {
    if (!done) {
      iterators.peek().next();
      lineUpIterators();
    }
    return (done == false);
  }

  protected void lineUpIterators() throws IOException {
    while (!allMatch() && !done) {
      ExtentValueIterator it = iterators.poll();
      it.next();
      iterators.offer(it);
      if (it.isDone()) {
        done = true;
        document = Integer.MAX_VALUE;
        return;
      }

      System.err.printf("Iterators lined up on doc % d\n", iterators.peek().currentIdentifier());

      if (!done) {
        document = iterators.peek().currentIdentifier();
        extents.reset();
        loadExtents();
        if (extents.getPositionCount() > 0) {
          return;
        }

        // Didn't find anything, so move one forward
        iterators.peek().next();
      }
    }
  }

  protected boolean allMatch() {
    int current = iterators.peek().currentIdentifier();
    for (ExtentValueIterator iterator : iterators) {
      if (iterator.isDone() || iterator.currentIdentifier() != current) {
        return false;
      }
    }
    return true;
  }

  public boolean isDone() {
    return done;
  }

  public boolean moveTo(int identifier) throws IOException {
    for (ValueIterator iterator : iterators) {
      iterator.moveTo(identifier);
    }
    if (allMatch()) {
      document = iterators.peek().currentIdentifier();
      return (document == identifier);
    } else {
      // missed it but need to line up
      next();
      return false;
    }
  }

  public void reset() throws IOException {
    for (ExtentValueIterator iterator : iterators) {
      iterator.reset();
    }
    done = false;
    if (!allMatch()) {
      next();
    }
  }

  public long totalEntries() {
    long min = Long.MAX_VALUE;
    for (ValueIterator iterator : iterators) {
      min = Math.min(min, iterator.totalEntries());
    }
    return min;
  }
}
