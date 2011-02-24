// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.extents;

import java.io.IOException;
import org.galagosearch.core.retrieval.structured.ScoreIterator;

/**
 *
 * @author trevor
 */
public class FakeScoreIterator extends ScoreIterator {

    int[] docs;
    double[] scores;
    int index;

    public FakeScoreIterator(int[] docs, double[] scores) {
        this.docs = docs;
        this.scores = scores;
        this.index = 0;
    }

    public int identifier() {
        return docs[index];
    }

    public boolean hasMatch(int document) {
        return document == docs[index];
    }

    public void moveTo(int document) throws IOException {
        while (!isDone() && document > docs[index]) {
            index++;
        }
    }

    public boolean skipToDocument(int document) throws IOException {
        moveTo(document);
        return this.hasMatch(document);
    }

    public void movePast(int document) throws IOException {
        while (!isDone() && document >= docs[index]) {
            index++;
        }
    }

    public double score() {
        return score(documentToScore, lengthOfDocumentToScore);
    }

    public double score(int document, int length) {
        if (docs[index] == document) {
            return scores[index];
        }
        return 0;
    }

    public boolean isDone() {
        return index >= docs.length;
    }

    public void reset() {
        index = 0;
    }
}
