/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

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

    public double score(int document, int length) {
        setScoringContext(document, length);
        return score();
    }

    public abstract boolean isDone();
    public abstract int currentCandidate();
    public abstract boolean hasMatch(int document);
    public abstract void moveTo(int document) throws IOException;
    public abstract void movePast(int document) throws IOException;
    public abstract boolean skipToDocument(int document) throws IOException;
    public abstract void reset() throws IOException;
    public abstract double score();

    // by default parameterSweeping is unsupported by any score iterator
    public Map<String, Double> parameterSweepScore() {
      throw new UnsupportedOperationException("Parameter sweep not supported for this score iterator.");
    }
}
