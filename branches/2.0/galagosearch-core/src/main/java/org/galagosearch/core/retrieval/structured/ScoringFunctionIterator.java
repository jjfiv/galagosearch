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
public class ScoringFunctionIterator extends ScoreIterator {

  boolean done;
  CountIterator iterator;
  ScoringFunction function;
  // parameter sweep functions
  ScoringFunction[] functions;
  long total;

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
    if (iterator.intID() == context.document) {
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
    if (iterator.intID() == context.document) {
      count = iterator.count();
    }
    TObjectDoubleHashMap<String> results = new TObjectDoubleHashMap();
    for (ScoringFunction f : functions) {
      results.put(f.getParameterString(), f.score(count, context.length));
    }
    return results;
  }

  public boolean moveTo(int id) throws IOException {
    return iterator.moveTo(id);
  }

  public boolean moveTo(long id) throws IOException {
    return iterator.moveTo(id);
  }

  public boolean moveTo(String id) throws IOException {
    return iterator.moveTo(id);
  }

  public void movePast(int document) throws IOException {
    iterator.movePast(document);
  }

  public void movePast(long id) throws IOException {
    iterator.movePast(id);
  }

  public void movePast(String id) throws IOException {
    iterator.movePast(id);
  }

  public int intID() {
    if (isDone()) {
      return Integer.MAX_VALUE;
    }
    return iterator.intID();
  }

  public long longID() {
    if (isDone()) {
      return Long.MAX_VALUE;
    }
    return iterator.longID();
  }

  public String stringID() {
    if (isDone()) {
      return null;
    }
    return iterator.stringID();
  }

  public boolean isDone() {
    return iterator.isDone();
  }

  public boolean hasMatch(int id) {
    return !isDone() && iterator.intID() == id;
  }

  public boolean hasMatch(long id) {
    return !isDone() && iterator.longID() == id;
  }

  public boolean hasMatch(String id) {
    return !isDone() && iterator.stringID() == id;
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

  public boolean nextEntry() throws IOException {
    return iterator.nextEntry();
  }

  public String getEntry() throws IOException {
    return iterator.getEntry();
  }

  public void reset(Iterator it) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
