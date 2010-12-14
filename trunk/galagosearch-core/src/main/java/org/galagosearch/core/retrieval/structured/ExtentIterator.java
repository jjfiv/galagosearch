// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.util.ExtentArray;

/**
 * This is base interface for all inverted lists that return count information.
 * See the CountIterator class for documentation on most of these methods.
 * 
 * @author trevor
 */
public abstract class ExtentIterator extends DocumentOrderedCountIterator {
    public abstract ExtentArray extents();

    @Override
    public boolean skipToDocument(int document) throws IOException {
        if (isDone()) {
            return false;
        }
        while (!isDone() && document() < document) {
            nextEntry();
        }
        return !isDone() && document == document();
    }
}
