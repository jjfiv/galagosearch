// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.scoring.ScoringFunction;

/**
 *
 * @author trevor
 */
public class ScoringFunctionIterator extends DocumentOrderedScoreIterator {
    boolean done;
    DocumentOrderedCountIterator iterator;
    ScoringFunction function;

    public ScoringFunctionIterator(DocumentOrderedCountIterator iterator, ScoringFunction function) {
        this.iterator = iterator;
        this.function = function;
    }

    public double score() {
        int count = 0;

        if (iterator.document() == documentToScore) {
            count = iterator.count();
        }
        return function.score(count, lengthOfDocumentToScore);
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

    public int currentCandidate() {
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

    public boolean skipToDocument(int document) throws IOException {
        return iterator.skipToDocument(document);
    }
}
