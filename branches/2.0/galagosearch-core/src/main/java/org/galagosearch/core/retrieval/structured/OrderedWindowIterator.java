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
public class OrderedWindowIterator extends ExtentConjunctionIterator {

  int width;

  /** Creates a new instance of OrderedWindowIterator */
  public OrderedWindowIterator(Parameters parameters, ExtentValueIterator[] iterators) throws IOException {
    super(iterators);
    this.width = (int) parameters.getAsDefault("width", -1);
    findDocument();
  }

  public void loadExtents() {
    ExtentArrayIterator[] arrayIterators;

    arrayIterators = new ExtentArrayIterator[iterators.length];
    for (int i = 0; i < iterators.length; i++) {
      arrayIterators[i] = new ExtentArrayIterator(iterators[i].extents());
    }
    boolean notDone = true;
    while (notDone) {
      // find the start of the first word
      boolean invalid = false;
      int begin = arrayIterators[0].current().begin;

      // loop over all the rest of the words
      for (int i = 1; i < arrayIterators.length; i++) {
        int end = arrayIterators[i - 1].current().end;

        // try to move this iterator so that it's past the end of the previous word
        assert(arrayIterators[i] != null);
        assert(arrayIterators[i].current() != null);
        while (end > arrayIterators[i].current().begin) {
          notDone = arrayIterators[i].next();

          // if there are no more occurrences of this word,
          // no more ordered windows are possible
          if (!notDone) {
            return;
          }
        }

        if (arrayIterators[i].current().begin - end >= width) {
          invalid = true;
          break;
        }
      }

      int end = arrayIterators[arrayIterators.length - 1].current().end;

      // if it's a match, record it
      if (!invalid) {
        extents.add(document, begin, end);
      }
      notDone = arrayIterators[0].next();
    }
  }
}
