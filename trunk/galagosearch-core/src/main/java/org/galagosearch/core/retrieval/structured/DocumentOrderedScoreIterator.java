// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import java.io.IOException;
import java.util.Map;

/**
 *
 * @author marc
 */
public abstract class DocumentOrderedScoreIterator implements DocumentOrderedIterator,
    ScoreIterator {

    /**
     * Shared variables that may be used in DAAT processing.
     */
    protected static int documentToScore = 0;
    protected static int lengthOfDocumentToScore = 0;

    public static void setScoringContext(int document, int length) {
        documentToScore = document;
        lengthOfDocumentToScore = length;
    }

    /**
     * Backoff method.
     * @return
     */
    public double maximumScore() {
        return Double.MAX_VALUE;
    }

    /**
     * Backoff method.
     *
     * @return
     */
    public double minimumScore() {
        return Double.MIN_VALUE;
    }

    public abstract boolean isDone();
    public abstract int currentCandidate();
    public abstract boolean hasMatch(int document);
    public abstract void moveTo(int document) throws IOException;
    public abstract void movePast(int document) throws IOException;
    public abstract boolean skipToDocument(int document) throws IOException;
    public abstract void reset() throws IOException;
    public abstract double score();
    public abstract double score(int document, int length); // use context defined by these variables

    // This provides an estimate of how many entries there are in the underlying iterator - implementation is optional
    public long totalCandidates() {
	return Long.MAX_VALUE;
    }

    // by default parameterSweeping is unsupported by any score iterator
    public TObjectDoubleHashMap<String> parameterSweepScore() {
      throw new UnsupportedOperationException("Parameter sweep not supported for this score iterator.");
    }
}
