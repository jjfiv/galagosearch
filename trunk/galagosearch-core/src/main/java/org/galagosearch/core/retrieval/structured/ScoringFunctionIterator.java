// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.scoring.ScoringFunction;

/**
 *
 * @author trevor
 */
public class ScoringFunctionIterator extends DocumentOrderedScoreIterator {

  boolean done;
  DocumentOrderedCountIterator iterator;
  ScoringFunction function;
  // parameter sweep functions
  ScoringFunction[] functions;

  public ScoringFunctionIterator(DocumentOrderedCountIterator iterator, ScoringFunction function) {
    this.iterator = iterator;
    this.function = function;
    this.functions = null; // null implies that we can not perform a parameter sweep
  }

  // if we have a set of functions -> (for parameter sweeping)
  public ScoringFunctionIterator(DocumentOrderedCountIterator iterator, ScoringFunction[] functions) {
    this.iterator = iterator;
    this.function = functions[0];
    this.functions = functions;
  }

  public double score() {
    int count = 0;

    if (iterator.document() == documentToScore) {
      count = iterator.count();
    }
    return function.score(count, lengthOfDocumentToScore);
  }

  @Override
  public Map<String, Double> parameterSweepScore() {
    if (functions == null) {
      throw new UnsupportedOperationException("Parameter sweep not supported for this score iterator.");
    }

    int count = 0;
    if (iterator.document() == documentToScore) {
      count = iterator.count();
    }
    HashMap<String, Double> results = new HashMap();
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
