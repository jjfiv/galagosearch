// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import java.io.IOException;
import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.core.scoring.ScoringFunction;

/**
 * An iterator that converts a count iterator's count into a score.
 * This is usually composed w/ a scoring function in order to produce an
 * appropriate score
 *
 * @author irmarc
 */
public class ScoringFunctionIterator extends DocumentOrderedScoreIterator {

  boolean done;
  DocumentOrderedCountIterator iterator;
  ScoringFunction function;
  // parameter sweep functions
  ScoringFunction[] functions;
  long total;

  public ScoringFunctionIterator(DocumentOrderedCountIterator iterator, ScoringFunction function) {
    this.iterator = iterator;
    this.function = function;
    this.functions = null; // null implies that we can not perform a parameter sweep
    if (PositionIndexReader.Iterator.class.isAssignableFrom(iterator.getClass())) {
      total = ((PositionIndexReader.Iterator) iterator).totalDocuments();
    } else {
      total = Long.MAX_VALUE;
    }
  }

  // if we have a set of functions -> (for parameter sweeping)
  public ScoringFunctionIterator(DocumentOrderedCountIterator iterator, ScoringFunction[] functions) {
    this(iterator, functions[0]);
    this.functions = functions;
  }

  public long totalCandidates() {
    return total;
  }

  public ScoringFunction getScoringFunction() {
    return function;
  }

  public double score() {
    return score(documentToScore, lengthOfDocumentToScore);
  }

  public double score(int document, int length) {
    int count = 0;

    // Used in counting # of score calls. Uncomment if you want to track that.
    //CallTable.increment("score_req");
    if (iterator.document() == document) {
      count = iterator.count();
    }
    return function.score(count, length);
  }

  @Override
  public TObjectDoubleHashMap<String> parameterSweepScore() {
    if (functions == null) {
      throw new UnsupportedOperationException("Parameter sweep not supported for this score iterator.");
    }

    int count = 0;
    if (iterator.document() == documentToScore) {
      count = iterator.count();
    }
    TObjectDoubleHashMap<String> results = new TObjectDoubleHashMap();
    for (ScoringFunction f : functions) {
      results.put(f.getParameterString(), f.score(count, lengthOfDocumentToScore));
    }
    return results;
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
