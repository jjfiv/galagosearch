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

  protected int document;
  protected boolean done;

  public ExtentConjunctionIterator(ExtentValueIterator[] extIterators) {
    this.done = false;
    iterators = new ExtentValueIterator[extIterators.length];
    for (int i = 0; i < extIterators.length; i++) {
      iterators[i] = extIterators[i];
    }
    this.extents = new ExtentArray();

    document = MoveIterators.findMaximumDocument(iterators);

    if (document == Integer.MAX_VALUE) {
      done = true;
    }
  }

  public int currentCandidate() {
    return document;
  }

  public boolean isDone() {
    return done;
  }

  /** moves all iterators to an identifier
   *  - current document is the max of it's children
   */
  public boolean moveTo(int identifier) throws IOException {
    extents.reset();

    for (ValueIterator iterator : iterators) {
      iterator.moveTo(identifier);
      if (iterator.isDone()) {
        done = true;
      }
    }

    document = MoveIterators.findMaximumDocument(iterators);

    if ((!isDone()) && MoveIterators.allHasMatch(iterators, document)) {
      // try to load some extents (subclass does this)
      extents.reset();
      loadExtents();
    }

    return hasMatch(identifier);
  }

  public void reset() throws IOException {
    for (ExtentIterator iterator : iterators) {
      iterator.reset();
    }
    document = MoveIterators.findMaximumDocument(iterators);
    done = false;
    extents.reset();
  }

  public long totalEntries() {
    long min = Long.MAX_VALUE;
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
    findDocument();
    return (!done);
  }

  private void findDocument() throws IOException {
    while (!done) {
      // find a document that might have some matches
      document = MoveIterators.moveAllToSameDocument(iterators);

      // if we're done, quit now
      if (document == Integer.MAX_VALUE) {
        done = true;
        break;
      }

      // try to load some extents (subclass does this)
      extents.reset();
      loadExtents();

      // were we successful? if so, quit, otherwise keep looking for documents
      if (extents.getPositionCount() > 0) {
        break;
      }
      iterators[0].movePast(document);
    }
  }
}
