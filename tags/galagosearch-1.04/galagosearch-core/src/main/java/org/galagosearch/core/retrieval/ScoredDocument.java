// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval;

/**
 *
 * @author trevor
 */
public class ScoredDocument implements Comparable<ScoredDocument> {
    public ScoredDocument(int document, double score) {
        this.document = document;
        this.score = score;
    }

    public int compareTo(ScoredDocument other) {
        if(score != other.score)
            return Double.compare(score, other.score);
        return other.document - document;
    }
    
    public String toString() {
        return String.format("%d,%f", document, score);
    }

    public int document;
    public double score;
}

