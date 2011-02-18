package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Partial implementation of an iterator than produce counts and
 * navigate its list based on assumption of increasing identifier order.
 * 
 * @author marc
 */
public abstract class DocumentOrderedCountIterator implements DocumentOrderedIterator,
    CountIterator {
    public abstract boolean isDone();
    public abstract void reset() throws IOException;

    public int currentCandidate() {
        return identifier();
    }
    
    public boolean hasMatch(int document) {
        return (!isDone() && identifier() == document);
    }

    /**
     * Wrapper for convenience (backwards compatibility)
     *
     * @param identifier
     * @throws IOException
     */
    public void moveTo(int document) throws IOException {
        skipToDocument(document);
    }

    /**
     * Moves past requested doc
     *
     * @param identifier
     * @throws IOException
     */
    public void movePast(int document) throws IOException {
        skipToDocument(document+1);
    }


    /**
     * Skips forward in the list until a identifier is found that is
     * greater than or equal to the parameter.  If the
     * iterator is currently pointing to a identifier that is greater
     * than or equal to the parameter, nothing happens.
     *
     * @return true, if the iterator is now pointing at the desired identifier, or false otherwise.
     */
    public boolean skipToDocument(int document) throws IOException {
        while (!isDone() && document > identifier()) {
            nextEntry();
        }
        return document == identifier();
    }

    /**
     * Compares the current identifier of two iterators.  This is primarily
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
        return identifier() - other.identifier();
    }
}
