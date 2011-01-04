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
public class ScaleIterator extends DocumentOrderedScoreIterator {

  DocumentOrderedScoreIterator iterator;
  double weight;
  double[] weights;

  public ScaleIterator(Parameters parameters, DocumentOrderedScoreIterator iterator) throws IllegalArgumentException {
    this.iterator = iterator;
    String[] weightStrings = parameters.get("default", "1.0").split(",");
    weight = Double.parseDouble(weightStrings[0]);

    // parameter sweep init
    weights = new double[weightStrings.length];
    for (int i = 0; i < weightStrings.length; i++) {
      weights[i] = Double.parseDouble(weightStrings[i]);
    }
  }

  public boolean skipToDocument(int document) throws IOException {
    return iterator.skipToDocument(document);
  }

  public int currentCandidate() {
    return iterator.currentCandidate();
  }

  public boolean hasMatch(int document) {
    return iterator.hasMatch(document);
  }

  public void moveTo(int document) throws IOException {
    iterator.moveTo(document);
  }

  public void movePast(int document) throws IOException {
    iterator.movePast(document);
  }

  public double score() {
    return weight * iterator.score();
  }

  public double score(int document, int length) {
    return weight * iterator.score(document, length);
  }

  /**
   *  Parameter Sweep Code
   */
  public TObjectDoubleHashMap<String> parameterSweepScore() {
    final TObjectDoubleHashMap<String> results = new TObjectDoubleHashMap();
    final TObjectDoubleHashMap<String> childResults = iterator.parameterSweepScore();
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

  public boolean isDone() {
    return iterator.isDone();
  }

  public void reset() throws IOException {
    iterator.reset();
  }
}
