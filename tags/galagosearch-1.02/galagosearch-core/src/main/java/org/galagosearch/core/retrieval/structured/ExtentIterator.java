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
public abstract class ExtentIterator extends CountIterator {
    public abstract void nextDocument() throws IOException;
    public abstract int document();
    public abstract int count();
    public abstract ExtentArray extents();
    public abstract boolean isDone();
    public abstract void reset() throws IOException;

    @Override
    public boolean skipToDocument(int document) throws IOException {
        if (isDone()) {
            return false;
        }
        while (!isDone() && document() < document) {
            nextDocument();
        }
        return !isDone() && document == document();
    }
}
