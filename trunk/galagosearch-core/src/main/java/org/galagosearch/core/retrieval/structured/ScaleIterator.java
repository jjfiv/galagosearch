// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class ScaleIterator extends DocumentOrderedScoreIterator {
    DocumentOrderedScoreIterator iterator;
    double weight;

    public ScaleIterator(Parameters parameters, DocumentOrderedScoreIterator iterator) throws IllegalArgumentException {
        this.iterator = iterator;
        weight = parameters.get("default", 1.0);
    }

    public boolean skipToDocument(int document) throws IOException {
        return iterator.skipToDocument(document);
    }

    public int currentCandidate() {
        return iterator.currentCandidate();
    }

    public boolean hasMatch(int document) {
        return iterator.hasMatch(document);
    }

    public void moveTo(int document) throws IOException {
        iterator.moveTo(document);
    }

    public void movePast(int document) throws IOException {
        iterator.movePast(document);
    }

    public double score() {
        return weight * iterator.score();
    }

    public boolean isDone() {
        return iterator.isDone();
    }

    public void reset() throws IOException {
        iterator.reset();
    }
}
