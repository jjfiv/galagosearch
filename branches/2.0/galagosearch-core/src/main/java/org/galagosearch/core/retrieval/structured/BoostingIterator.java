package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 * The internal iterator is expected to be an indicator
 * iterator. This performs a transform into the "score space"
 * by emitting a boost score (beta) iff the indicator is on.
 * @author irmarc
 */
public class BoostingIterator extends TransformIterator {

  double beta;

  public BoostingIterator(Parameters p, IndicatorIterator inner) {
    super((ValueIterator)inner);
    beta = p.get("beta", 0.5D);
  }


  public double score() {
    if (((IndicatorIterator) iterator).getStatus()) {
      return beta;
    } else {
      return 0.0;
    }
  }

  public double score(DocumentContext context) {
    if (((IndicatorIterator) iterator).getStatus()) {
      return beta;
    } else {
      return 0.0;
    }
  }

  public double maximumScore() {
    return beta;
  }

  public double minimumScore() {
    return 0.0;
  }

  public TObjectDoubleHashMap<String> parameterSweepScore() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
