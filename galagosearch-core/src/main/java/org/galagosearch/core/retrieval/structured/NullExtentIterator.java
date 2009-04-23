// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.util.ExtentArray;

/**
 *
 * @author trevor
 */
public class NullExtentIterator extends ExtentIterator {
    ExtentArray array = new ExtentArray();

    public void nextDocument() {
    }

    public boolean isDone() {
        return true;
    }

    public ExtentArray extents() {
        return array;
    }

    public int document() {
        return Integer.MAX_VALUE;
    }

    public int count() {
        return 0;
    }

    public void reset() {
        // do nothing
    }
}
