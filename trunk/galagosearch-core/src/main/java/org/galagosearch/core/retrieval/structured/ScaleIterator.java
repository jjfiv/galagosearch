// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class ScaleIterator implements ScoreIterator {
    ScoreIterator iterator;
    double weight;

    public ScaleIterator(Parameters parameters, ScoreIterator iterator) throws IllegalArgumentException {
        this.iterator = iterator;
        weight = parameters.get("weight", 1.0);
    }

    public int nextCandidate() {
        return iterator.nextCandidate();
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

    public double score(int document, int length) {
        return weight * iterator.score(document, length);
    }

    public boolean isDone() {
        return iterator.isDone();
    }

    public void reset() throws IOException {
        iterator.reset();
    }
}
