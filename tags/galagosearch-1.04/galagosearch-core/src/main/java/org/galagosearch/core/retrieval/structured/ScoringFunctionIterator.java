// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public abstract class ScoringFunctionIterator implements ScoreIterator {
    boolean done;
    CountIterator iterator;

    public ScoringFunctionIterator(CountIterator iterator) {
        this.iterator = iterator;
    }

    public abstract double scoreCount(int count, int length);

    public double score(int document, int length) {
        int count = 0;

        if (iterator.document() == document) {
            count = iterator.count();
        }
        return scoreCount(count, length);
    }

    public void moveTo(int document) throws IOException {
        if (!iterator.isDone()) {
            iterator.skipToDocument(document);
        }
    }

    public void movePast(int document) throws IOException {
        if (!iterator.isDone() && iterator.document() <= document) {
            iterator.skipToDocument(document + 1);
        }
    }

    public int nextCandidate() {
        if (isDone()) {
            return Integer.MAX_VALUE;
        }
        return iterator.document();
    }

    public boolean isDone() {
        return iterator.isDone();
    }

    public boolean hasMatch(int document) {
        return !isDone() && iterator.document() == document;
    }

    public void reset() throws IOException {
        iterator.reset();
    }
}
