// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval;

/**
 *
 * @author trevor, irmarc, sjh
 */
public class ScoredDocument implements Comparable<ScoredDocument> {

  public ScoredDocument() {
    this(0, 0);
  }

  public ScoredDocument(int document, double score) {
    this.document = document;
    this.score = score;
  }

  public int compareTo(ScoredDocument other) {
    if (score != other.score) {
      return Double.compare(score, other.score);
    }
    if( (source != null) &&
        (! source.equals(other.source))){
      return source.compareTo(other.source);
    }
    return other.document - document;
  }

  public String toString() {
    return String.format("%d,%f", document, score);
  }

  public String documentName;
  public String source; // lets us know where this scored doc came from
  public int document;
  public int rank;
  public double score;

  // keeps track of the parameters that were used to score this document - only used for parameterSweeping
  public String params;
}
