// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.extents;

import gnu.trove.TObjectDoubleHashMap;
import java.io.IOException;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.retrieval.structured.DocumentContext;
import org.galagosearch.core.retrieval.structured.ScoreValueIterator;

/**
 *
 * @author trevor
 */
public class FakeScoreIterator implements ScoreValueIterator {

  int[] docs;
  double[] scores;
  int index;
  DocumentContext context;

  public FakeScoreIterator(int[] docs, double[] scores) {
    this.docs = docs;
    this.scores = scores;
    this.index = 0;
  }

  public int currentIdentifier() {
    return docs[index];
  }

  public boolean hasMatch(int document) {
    if (isDone()) return false;
    else return document == docs[index];
  }

  public boolean moveTo(int document) throws IOException {
    while (!isDone() && document > docs[index]) {
      index++;
    }
    return (hasMatch(document));
  }

  public void movePast(int document) throws IOException {
    while (!isDone() && document >= docs[index]) {
      index++;
    }
  }

  public double score() {
    return score(context);
  }

  public double score(DocumentContext dc) {
    if (docs[index] == dc.document) {
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

  public double maximumScore() {
    return Double.POSITIVE_INFINITY;
  }

  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }

  public TObjectDoubleHashMap<String> parameterSweepScore() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public DocumentContext getContext() {
    return context;
  }

  public void setContext(DocumentContext context) {
    this.context = context;
  }

  public boolean next() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public long totalEntries() {
    return docs.length;
  }

  public int compareTo(ValueIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return currentIdentifier() - other.currentIdentifier();
  }
}
