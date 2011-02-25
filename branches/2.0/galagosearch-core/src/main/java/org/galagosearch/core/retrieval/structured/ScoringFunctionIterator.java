// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.index.GenericIndexReader.Iterator;
import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.core.scoring.ScoringFunction;
import org.galagosearch.core.util.CallTable;

/**
 * An iterator that converts a count iterator's count into a score.
 * This is usually composed w/ a scoring function in order to produce an
 * appropriate score
 *
 * @author irmarc
 */
public class ScoringFunctionIterator implements ScoreIterator, ContextualIterator {

  boolean done;
  CountIterator iterator;
  ScoringFunction function;
  // parameter sweep functions
  ScoringFunction[] functions;
  long total;
  DocumentContext context;

  public ScoringFunctionIterator(CountIterator iterator, ScoringFunction function) throws IOException {
    this.iterator = iterator;
    this.function = function;
    this.functions = null; // null implies that we can not perform a parameter sweep
    total = iterator.totalEntries();
  }

  // if we have a set of functions -> (for parameter sweeping)
  public ScoringFunctionIterator(CountIterator iterator, ScoringFunction[] functions) throws IOException {
    this(iterator, functions[0]);
    this.functions = functions;
  }

  public void setContext(DocumentContext dc) {
    context = dc;
  }

  public DocumentContext getContext() {
    return context;
  }

  public long totalCandidates() {
      return total;
  }

  public ScoringFunction getScoringFunction() {
      return function;
  }

  public double score() {
    int count = 0;

    // Used in counting # of score calls. Uncomment if you want to track that.
    //CallTable.increment("score_req");
    if (iterator.currentIdentifier() == context.document) {
      count = iterator.count();
    }
    return function.score(count, context.length);
  }

  @Override
  public TObjectDoubleHashMap<String> parameterSweepScore() {
    if (functions == null) {
      throw new UnsupportedOperationException("Parameter sweep not supported for this score iterator.");
    }

    int count = 0;
    if (iterator.currentIdentifier() == context.document) {
      count = iterator.count();
    }
    TObjectDoubleHashMap<String> results = new TObjectDoubleHashMap();
    for (ScoringFunction f : functions) {
      results.put(f.getParameterString(), f.score(count, context.length));
    }
    return results;
  }

  public int currentIdentifier() {
    if (isDone()) {
      return Integer.MAX_VALUE;
    }
    return iterator.currentIdentifier();
  }

  public boolean isDone() {
    return iterator.isDone();
  }

  public boolean hasMatch(int id) {
    return !isDone() && iterator.hasMatch(id);
  }

  public void reset() throws IOException {
    iterator.reset();
  }

  public double maximumScore() {
    return Double.POSITIVE_INFINITY;
  }

  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }

  public long totalEntries() throws IOException {
    return iterator.totalEntries();
  }

  public void next() {
    try {
      iterator.nextEntry();
    } catch (IOException ioe)  {
      throw new RuntimeException(ioe);
    }
  }

  public String getEntry() throws IOException {
    return iterator.getEntry();
  }
}
