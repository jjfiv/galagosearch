// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 * This is base interface for all inverted lists that return count information.
 *
 * @see ExtentIterator
 * @author trevor
 */
public abstract class CountIterator implements StructuredIterator, Comparable<CountIterator> {
    /**
     * Skips forward in the list until a document is found that is
     * greater than or equal to the parameter.  If the
     * iterator is currently pointing to a document that is greater
     * than or equal to the parameter, nothing happens.
     * 
     * @return true, if the iterator is now pointing at the desired document, or false otherwise.
     */
    public boolean skipToDocument(int document) throws IOException {
        while (!isDone() && document > document()) {
            nextDocument();
        }
        return document == document();
    }

    /**
     * Moves the iterator to the next document in the list.  If
     * no such document is available, isDone() will return true
     * after this method is called.
     */
    public abstract void nextDocument() throws IOException;

    /**
     * Returns the current document.
     */
    public abstract int document();

    /**
     * Returns the number of occurrences of this iterator's term in
     * the current document.
     */
    public abstract int count();

    /**
     * Returns true if there is no data left in the list and false otherwise.
     * If the result of this method is true, the data returned from document()
     * and count() is meaningless.
     */
    public abstract boolean isDone();

    /**
     * Resets the position of this iterator to the start of the list.
     */
    public abstract void reset() throws IOException;

    /**
     * Compares the current document of two iterators.  This is primarily
     * useful for adding iterators to a PriorityQueue object for use during
     * retrieval.
     * 
     * @see PriorityQueue
     */
    public int compareTo(CountIterator other) {
        if (isDone() && !other.isDone()) {
            return 1;
        }
        if (other.isDone() && !isDone()) {
            return -1;
        }
        if (isDone() && other.isDone()) {
            return 0;
        }
        return document() - other.document();
    }
}
