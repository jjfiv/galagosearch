// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class UnorderedWindowIterator extends ExtentConjunctionIterator {

  int width;
  boolean overlap;

  /** Creates a new instance of UnorderedWindowIterator */
  public UnorderedWindowIterator(Parameters parameters, ExtentValueIterator[] evIterators) throws IOException {
    super(evIterators);
    this.width = (int) parameters.getAsDefault("width", -1);
    this.overlap = parameters.get("overlap", false);
    lineUpIterators();
  }

  public void loadExtents() {
    System.err.println("UW loadExtents");
    extents.reset();

    ExtentArrayIterator[] arrayIterators;
    int maximumPosition = 0;
    int minimumPosition = Integer.MAX_VALUE;

    // someday this will be a heap/priorityQueue for the overlapping case
    arrayIterators = new ExtentArrayIterator[iterators.size()];

    int i = 0;
    for (ExtentValueIterator iterator : iterators) {
      arrayIterators[i] = new ExtentArrayIterator(iterator.extents());
      minimumPosition = Math.min(arrayIterators[i].current().begin, minimumPosition);
      maximumPosition = Math.max(arrayIterators[i].current().end, maximumPosition);
      i++;
    }

    do {
      boolean match = (maximumPosition - minimumPosition <= width);
      System.err.printf("minimum=%d, maximum=%d, width=%d, match=%b\n", minimumPosition,
              maximumPosition, width, match);
      // try to emit an extent here, but only if the width is small enough
      if (match) {
        extents.add(document, minimumPosition, maximumPosition);
        System.err.printf("Adding extent: %d, %d, %d\n", document, minimumPosition, maximumPosition);
      }
      if (overlap || !match) {
        // either it didn't just match or we don't care about overlap,
        // so we want to increment only the very first iterator
        for (i = 0; i < arrayIterators.length; i++) {
          if (arrayIterators[i].current().begin == minimumPosition) {
            boolean result = arrayIterators[i].next();

            if (!result) {
              return;
            }
          }
        }
      } else {
        // last was a match, so increment all iterators past the end of the match
        for (i = 0; i < arrayIterators.length; i++) {
          while (arrayIterators[i].current().begin < maximumPosition) {
            boolean result = arrayIterators[i].next();

            if (!result) {
              return;
            }
          }
        }
      }

      // reset the minimumPosition
      minimumPosition = Integer.MAX_VALUE;
      maximumPosition = 0;

      // now, reset bounds
      for (i = 0; i < arrayIterators.length; i++) {
        minimumPosition = Math.min(minimumPosition, arrayIterators[i].current().begin);
        maximumPosition = Math.max(maximumPosition, arrayIterators[i].current().end);
      }
    } while (true);
  }
}
