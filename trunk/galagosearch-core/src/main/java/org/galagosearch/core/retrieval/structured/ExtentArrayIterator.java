// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.util.ExtentArray;

/**
 *
 * @author trevor
 */
public class ExtentArrayIterator implements Comparable<ExtentArrayIterator> {
    ExtentArray array;
    int index;

    public ExtentArrayIterator(ExtentArray array) {
        this.array = array;
    }

    public Extent current() {
        return array.getBuffer()[index];
    }

    public boolean next() {
        index += 1;
        return index < array.getPosition();
    }

    public boolean isDone() {
        return array.getPosition() <= index;
    }

    public int compareTo(ExtentArrayIterator iterator) {
        int result = current().document - iterator.current().document;

        if (result != 0) {
            return result;
        }
        return current().begin - iterator.current().begin;
    }
}
