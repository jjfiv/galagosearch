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
  double[] weights;

  public ScaleIterator(Parameters parameters, ScoreValueIterator iterator) throws IllegalArgumentException {
    super(iterator);
    String[] weightStrings = parameters.get("default", "1.0").split(",");
    weight = Double.parseDouble(weightStrings[0]);

    // parameter sweep init
    weights = new double[weightStrings.length];
    for (int i = 0; i < weightStrings.length; i++) {
      weights[i] = Double.parseDouble(weightStrings[i]);
    }
  }
  
  public double score() {
    return weight * ((ScoreIterator)iterator).score();
  }

  public double score(DocumentContext context) {
    return weight * ((ScoreIterator)iterator).score(context);
  }

  /**
   *  Parameter Sweep Code
   */
  public TObjectDoubleHashMap<String> parameterSweepScore() {
    final TObjectDoubleHashMap<String> results = new TObjectDoubleHashMap();
    final TObjectDoubleHashMap<String> childResults = ((ScoreIterator)iterator).parameterSweepScore();
    for (int i = 0; i < weights.length; i++) {
      final int j = i;
      childResults.forEachKey(new TObjectProcedure<String>() {

        public boolean execute(String childParam) {
          double r = weights[j] * childResults.get(childParam);
          StringBuilder p = new StringBuilder("#scale:");
          p.append(weights[j]);
          p.append("(");
          p.append(childParam);
          p.append(")");
          results.put(p.toString(), r);
          return true;
        }
      });
    }
    return results;
  }

  public double maximumScore() {
    return ((ScoreIterator)iterator).maximumScore();
  }

  public double minimumScore() {
    return ((ScoreIterator)iterator).minimumScore();
  }
}
