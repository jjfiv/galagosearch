package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Partial implementation of an iterator than produce counts and
 * navigate its list based on assumption of increasing document order.
 * 
 * @author marc
 */
public abstract class DocumentOrderedCountIterator implements DocumentOrderedIterator,
    CountIterator {
    public abstract boolean isDone();
    public abstract void reset() throws IOException;

    public int currentCandidate() {
        return document();
    }
    
    public boolean hasMatch(int document) {
        return (!isDone() && document() == document);
    }

    /**
     * Wrapper for convenience (backwards compatibility)
     *
     * @param document
     * @throws IOException
     */
    public void moveTo(int document) throws IOException {
        skipToDocument(document);
    }

    /**
     * Moves past requested doc
     *
     * @param document
     * @throws IOException
     */
    public void movePast(int document) throws IOException {
        skipToDocument(document+1);
    }


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
            nextEntry();
        }
        return document == document();
    }

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
