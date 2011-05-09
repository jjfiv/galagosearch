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
    public UnorderedWindowIterator(Parameters parameters, ExtentIterator[] extentIterators) throws IOException {
        super(extentIterators);
        this.width = (int) parameters.getAsDefault("width", -1);
        this.overlap = parameters.get("overlap", false);
        findDocument();
    }

    public void loadExtents() {
        extents.reset();

        ExtentArrayIterator[] iterators;
        int maximumPosition = 0;
        int minimumPosition = Integer.MAX_VALUE;

        // someday this will be a heap/priorityQueue for the overlapping case
        iterators = new ExtentArrayIterator[extentIterators.length];

        for (int i = 0; i < extentIterators.length; i++) {
            iterators[i] = new ExtentArrayIterator(extentIterators[i].extents());
            minimumPosition = Math.min(iterators[i].current().begin, minimumPosition);
            maximumPosition = Math.max(iterators[i].current().end, maximumPosition);
        }

        do {
            boolean match = (maximumPosition - minimumPosition <= width);

            // try to emit an extent here, but only if the width is small enough
            if (match) {
                extents.add(document, minimumPosition, maximumPosition);
            }
            if (overlap || !match) {
                // either it didn't just match or we don't care about overlap,
                // so we want to increment only the very first iterator
                for (int i = 0; i < iterators.length; i++) {
                    if (iterators[i].current().begin == minimumPosition) {
                        boolean result = iterators[i].next();

                        if (!result) {
                            return;
                        }
                    }
                }
            } else {
                // last was a match, so increment all iterators past the end of the match
                for (int i = 0; i < iterators.length; i++) {
                    while (iterators[i].current().begin < maximumPosition) {
                        boolean result = iterators[i].next();

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
            for (int i = 0; i < iterators.length; i++) {
                minimumPosition = Math.min(minimumPosition, iterators[i].current().begin);
                maximumPosition = Math.max(maximumPosition, iterators[i].current().end);
            }
        } while (true);
    }
}
