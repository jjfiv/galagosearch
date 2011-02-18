package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 * Anything implementing this interface
 * has the ability to advance forward based on impacts/scores.
 *
 * TODO (irmarc): Determine if it makes sense to implement skipping here,
 *                since you have the max score available.
 *
 * @author irmarc
 */
public interface ImpactOrderedIterator extends StructuredIterator {
    public boolean isDone();

    /**
     * This should return the current candidate document. Changes after
     * a call to nextEntry.
     *
     * @return
     */
    public int currentCandidate();

    /**
     * Move the iterator forward.
     * @throws IOException
     */
    public void nextEntry() throws IOException;
}
