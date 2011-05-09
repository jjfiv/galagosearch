// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.util.ExtentArray;

/**
 * This class is meant to be a base class for many kinds of
 * iterators that require at least one of their children to be
 * present in the intID for a match to happen.  This class
 * will call loadExtents once for each id that has a
 * match on any child iterator.
 * 
 * @author trevor
 */
public abstract class ExtentDisjunctionIterator extends ExtentCombinationIterator {

  protected PriorityQueue<ExtentValueIterator> activeIterators;
  protected int document;

  public ExtentDisjunctionIterator(ExtentValueIterator[] iterators) {
    this.iterators = iterators;
    this.activeIterators = new PriorityQueue<ExtentValueIterator>(iterators.length);

    this.extents = new ExtentArray();
    this.document = 0;

    for (ExtentValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        this.activeIterators.add(iterator);
      }
    }
  }

  public int currentCandidate() {
    return document;
  }

  public boolean isDone() {
    return activeIterators.size() == 0;
  }

  /**
   * Moves the lowest iterator forward until it's equal to or greater than
   * the supplied identifier.
   * @param identifier
   * @return
   */
  public boolean moveTo(int identifier) throws IOException {
    boolean hit = false;
    for (ValueIterator iterator : activeIterators) {
      iterator.moveTo(identifier);
    }
    if (!isDone()) {
      document = activeIterators.peek().currentCandidate();
      for (ValueIterator iterator : activeIterators) {
        iterator.moveTo(document); // necessary for conjunctions
      }
      extents.reset();
      loadExtents();
      return true;
    } else {
      document = Integer.MAX_VALUE;
      return false;
    }
  }

  public void reset() throws IOException {
    activeIterators.clear();
    for (ExtentValueIterator iterator : iterators) {
      iterator.reset();
      if (!iterator.isDone()) {
        activeIterators.add(iterator);
      }
    }

    loadExtents();
  }

  public long totalEntries() {
    long max = 0;
    for (ValueIterator iterator : activeIterators) {
      max = Math.max(max, iterator.totalEntries());
    }
    return max;
  }


  /**
   *  *** BE VERY CAREFUL IN CALLING THIS FUNCTION ***
   */
  public boolean next() throws IOException {
    // find all activeIterators on the current id and move them forward
    while (activeIterators.size() > 0 && activeIterators.peek().currentCandidate() == document) {
      ExtentValueIterator iter = activeIterators.poll();
      iter.movePast(document);

      if (!iter.isDone()) {
        activeIterators.offer(iter);
      }
    }

    if (!isDone()) {
      document = activeIterators.peek().currentCandidate();
      extents.reset();
      loadExtents();
      return true;
    } else {
      document = Integer.MAX_VALUE;
      return false;
    }
  }
}
