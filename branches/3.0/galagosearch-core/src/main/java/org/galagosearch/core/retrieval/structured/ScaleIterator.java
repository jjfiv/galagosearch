// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectProcedure;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class ScaleIterator extends TransformIterator {

  double weight;

  public ScaleIterator(Parameters parameters, ScoreValueIterator iterator) throws IllegalArgumentException {
    super(iterator);
    weight = Double.parseDouble(parameters.get("default", "1.0"));
  }
  
  public double score() {
    return weight * ((ScoreIterator)iterator).score();
  }

  public double score(DocumentContext context) {
    return weight * ((ScoreIterator)iterator).score(context);
  }

  public double maximumScore() {
    return ((ScoreIterator)iterator).maximumScore();
  }

  public double minimumScore() {
    return ((ScoreIterator)iterator).minimumScore();
  }
}
