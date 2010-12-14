package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 * Anything implementing this interface
 * has all of the necessary methods for navigation based on
 * document ids.
 *
 * @author irmarc
 */
public interface DocumentOrderedIterator extends StructuredIterator {
    public boolean isDone();
    public int currentCandidate();
    public boolean hasMatch(int document);
    public void moveTo(int document) throws IOException;
    public void movePast(int document) throws IOException;
    public boolean skipToDocument(int document) throws IOException;
}
