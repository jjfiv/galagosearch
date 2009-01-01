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

    /** Creates a new instance of UnorderedWindowIterator */
    public OrderedWindowIterator(Parameters parameters, ExtentIterator[] iterators) throws IOException {
        super(iterators);
        this.width = (int) parameters.getAsDefault("width", -1);
        findDocument();
    }

    public void loadExtents() {
        ExtentArrayIterator[] iterators;

        iterators = new ExtentArrayIterator[extentIterators.length];
        for (int i = 0; i < extentIterators.length; i++) {
            iterators[i] = new ExtentArrayIterator(
                    extentIterators[i].extents());
        }
        boolean notDone = true;
        while (notDone) {
            // find the start of the first word
            boolean invalid = false;
            int begin = iterators[0].current().begin;

            // loop over all the rest of the words
            for (int i = 1; i < iterators.length; i++) {
                int end = iterators[i - 1].current().end;

                // try to move this iterator so that it's past the end of the previous word
                while (end > iterators[i].current().begin) {
                    notDone = iterators[i].next();

                    // if there are no more occurrences of this word,
                    // no more ordered windows are possible
                    if (!notDone) {
                        return;
                    }
                }

                if (iterators[i].current().begin - end >= width) {
                    invalid = true;
                    break;
                }
            }

            int end = iterators[iterators.length - 1].current().end;

            // if it's a match, record it
            if (!invalid) {
                extents.add(document, begin, end);
            }
            notDone = iterators[0].next();
        }
    }
}
