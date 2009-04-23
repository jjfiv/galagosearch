// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.ScoreIterator;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class FakeScoreIterator implements ScoreIterator {
    int[] docs;
    double[] scores;
    int index;
    
    public FakeScoreIterator( int[] docs, double[] scores ) {
        this.docs = docs;
        this.scores = scores;
        this.index = 0;
    }
    
    public int nextCandidate() {
        return docs[index];
    }

    public boolean hasMatch(int document) {
        return document == docs[index];
    }

    public void moveTo(int document) throws IOException {
        while( !isDone() && document > docs[index] )
            index++;
    }

    public void movePast(int document) throws IOException {
        while( !isDone() && document >= docs[index] )
            index++;
    }

    public double score(int document, int length ) {
        if( docs[index] == document )
            return scores[index];
        return 0;
    }

    public boolean isDone() {
        return index >= docs.length;
    }
    
    public void reset() {
        index = 0;
    }
}
