// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public interface ScoreIterator extends StructuredIterator {
    public int nextCandidate();
    public boolean hasMatch(int document);
    public void moveTo(int document) throws IOException;
    public void movePast(int document) throws IOException;
    public double score(int document, int length);
    public boolean isDone();
    public void reset() throws IOException;
}
