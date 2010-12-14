// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 * 12/11/2010 (irmarc): Refactored to represent anything that
 * iterates and produces scores.
 *
 * @author trevor, irmarc
 */
public interface ScoreIterator extends StructuredIterator {

    /**
     * Produce a score for the current candidate
     * @return
     */
    public double score();

    /**
     * The next document to be scored by this iterator
     * @return
     */
    public int currentCandidate();
}
