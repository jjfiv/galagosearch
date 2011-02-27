// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public class MoveIterators {
    /**
     * Moves all iterators in the array to the same intID.  This method will only
     * stop at documents where all of the iterators have a match.
     *
     * This code assumes that the most selective iterator (the one with the fewest intID
     * matches) is the first one.
     *
     * @return The currentIdentifier number that the iterators are now pointing to, or Integer.MAX_VALUE
     *         if one of the iterators is now done.
     */
    public static int moveAllToSameDocument(ExtentIterator[] iterators) throws IOException {
        if (iterators.length == 0) {
            return Integer.MAX_VALUE;
        }
        if (iterators[0].isDone()) {
            return Integer.MAX_VALUE;
        }
        int currentTarget = iterators[0].currentIdentifier();
        boolean allMatch = false;

        retry:
        while (!allMatch) {
            allMatch = true;

            for (ExtentIterator iterator : iterators) {
                if (iterator.isDone()) {
                    return Integer.MAX_VALUE;
                }
                int thisDocument = iterator.currentIdentifier();

                // this iterator points somewhere before our
                // current target currentIdentifier, so try to move forward to
                // the target
                if (currentTarget > thisDocument) {
                    iterator.skipToDocument(currentTarget);
                    if (iterator.isDone()) {
                        return Integer.MAX_VALUE;
                    }
                    thisDocument = iterator.currentIdentifier();
                }

                // this iterator points after the target currentIdentifier,
                // so the target currentIdentifier is not a match.
                // we break and try again because we don't want to 
                // touch the longest iterators if we can help it.
                if (currentTarget < thisDocument) {
                    allMatch = false;
                    currentTarget = thisDocument;
                    continue retry;
                }
            }
        }

        return currentTarget;
    }

    public static boolean allSameDocument(ExtentIterator[] iterators) {
        if (iterators.length == 0) {
            return true;
        }
        int document = iterators[0].currentIdentifier();

        for (ExtentIterator iterator : iterators) {
            if (document != iterator.currentIdentifier()) {
                return false;
            }
        }

        return true;
    }

    public static int findMaximumDocument(ExtentIterator[] iterators) {
        int maximumDocument = 0;

        for (ExtentIterator iterator : iterators) {
            if (iterator.isDone()) {
                return Integer.MAX_VALUE;
            }
            maximumDocument = Math.max(maximumDocument, iterator.currentIdentifier());
        }

        return maximumDocument;
    }

    public static int findMinimumDocument(ExtentIterator[] iterators) {
        int minimumDocument = Integer.MAX_VALUE;

        for (ExtentIterator iterator : iterators) {
            minimumDocument = Math.min(minimumDocument, iterator.currentIdentifier());
        }

        return minimumDocument;
    }

    public static int findMinimumCandidate(ScoreIterator[] iterators) {
        int minimumDocument = Integer.MAX_VALUE;

        for (ScoreIterator iterator : iterators) {
            minimumDocument = Math.min(minimumDocument, iterator.identifier());
        }

        return minimumDocument;
    }
}
