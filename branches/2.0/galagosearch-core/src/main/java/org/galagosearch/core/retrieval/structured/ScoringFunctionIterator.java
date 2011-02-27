// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.index.GenericIndexReader.Iterator;
import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.scoring.ScoringFunction;
import org.galagosearch.core.util.CallTable;

/**
 * An iterator that converts a count iterator's count into a score.
 * This is usually composed w/ a scoring function in order to produce an
 * appropriate score
 *
 * @author irmarc
 */
public class ScoringFunctionIterator extends TransformIterator {

  boolean done;
  ScoringFunction function;
  // parameter sweep functions
  ScoringFunction[] functions;
  long total;

  public ScoringFunctionIterator(CountValueIterator iterator, ScoringFunction function) throws IOException {
    super(iterator);
    this.function = function;
    this.functions = null; // null implies that we can not perform a parameter sweep
    System.err.printf("%s wraps %s\n", this.toString(), iterator.toString());
  }

  // if we have a set of functions -> (for parameter sweeping)
  public ScoringFunctionIterator(CountValueIterator iterator, ScoringFunction[] functions) throws IOException {
    this(iterator, functions[0]);
    this.functions = functions;
  }

  public ScoringFunction getScoringFunction() {
    return function;
  }

  public double score(DocumentContext dc) {
    int count = 0;

    // Used in counting # of score calls. Uncomment if you want to track that.
    //CallTable.increment("score_req");
    if (iterator.currentIdentifier() == dc.document) {
      count = ((CountIterator)iterator).count();
    }
    return function.score(count, dc.length);
  }

  public double score() {
    int count = 0;

    // Used in counting # of score calls. Uncomment if you want to track that.
    //CallTable.increment("score_req");
    System.err.printf("%s: told to score under context (%d, %d). current doc = %d\n", this.toString(),
		      context.document, context.length, iterator.currentIdentifier());
    if (iterator.currentIdentifier() == context.document) {
      count = ((CountIterator)iterator).count();
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
      count = ((CountIterator)iterator).count();
    }
    TObjectDoubleHashMap<String> results = new TObjectDoubleHashMap();
    for (ScoringFunction f : functions) {
      results.put(f.getParameterString(), f.score(count, context.length));
    }
    return results;
  }

  public double maximumScore() {
    return Double.POSITIVE_INFINITY;
  }

  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }
}
