// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 * #filter ( AbstractIndicator ScoreIterator ) : Only scores documents that where the AbstractIndicator is on
 *
 * @author sjh
 */
public class FilteredIterator implements ScoreValueIterator {

  DocumentContext context;
  IndicatorIterator indicator;
  ScoreValueIterator scorer;
  int document;

  public FilteredIterator(Parameters parameters, IndicatorIterator indicator, ScoreValueIterator scorer) {
    this.indicator = indicator;
    this.scorer = scorer;

    document = scorer.currentCandidate();
  }

  public double score() {
    return score(this.context);
  }

  public double score(DocumentContext context) {
    if (this.hasMatch(context.document)) {
      return scorer.score();
    } else {
      return Double.NEGATIVE_INFINITY;
    }
  }

  public double maximumScore() {
    return scorer.maximumScore();
  }

  public double minimumScore() {
    return scorer.minimumScore();
  }

  public void setContext(DocumentContext context) {
    this.context = context;
  }

  public DocumentContext getContext() {
    return this.context;
  }

  public void reset() throws IOException {
    indicator.reset();
    scorer.reset();
    this.document = scorer.currentCandidate();
  }

  public boolean isDone() {
    return scorer.isDone();
  }

  public int currentCandidate() {
    return document;
  }

  public boolean hasMatch(int identifier) {
    if (this.document == identifier) {
      return indicator.getStatus( identifier );
    } else {
      return false;
    }
  }
  
  /* 
   *  BE VERY CAREFUL NOT TO CALL next() INTERNALLY
   */
  public boolean next() throws IOException {
    movePast(document);
    // use a new doccontext to iterate until the indicator is true
    while((! isDone()) && (! this.hasMatch( document ))){
      movePast(document);
    }
    return (! isDone() );
  }

  public boolean moveTo(int identifier) throws IOException {
    if (!scorer.isDone()) {
      scorer.moveTo(identifier);
      document = scorer.currentCandidate();
    }

    if (!indicator.isDone()) {
      indicator.moveTo(identifier);
      // indicator may or may not point to the same document.
      // if not: it should have a default status to return.
    }
    return hasMatch(identifier);
  }

  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Filter nodes don't have singular values");
  }

  public long totalEntries() {
    return Math.min(indicator.totalEntries(), scorer.totalEntries());
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
    return currentCandidate() - other.currentCandidate();
  }
}
