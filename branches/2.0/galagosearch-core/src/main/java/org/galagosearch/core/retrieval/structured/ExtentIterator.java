// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.util.ExtentArray;

/**
 * This extends a identifier-ordered navigable count iterator by returning
 * arrays of extents, each of which is a position range (start - end), docid, and
 * weight.
 * 
 * @author trevor, irmarc
 */
public abstract class ExtentIterator extends DocumentOrderedCountIterator {
    public abstract ExtentArray extents();

    @Override
    public boolean skipToDocument(int document) throws IOException {
        if (isDone()) {
            return false;
        }
        while (!isDone() && identifier() < document) {
            nextEntry();
        }
        return !isDone() && document == identifier();
    }
}
