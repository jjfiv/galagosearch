/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.scoring.JelinekMercerScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author marc
 */
@RequiredStatistics(statistics = {"collectionLength"})
public class JelinekMercerScoringIterator extends ScoringFunctionIterator {
    public JelinekMercerScoringIterator(Parameters p, DocumentOrderedCountIterator it)
            throws IOException {
    super(it, makeFunctions(p, it));
  }
  /** Parameter Sweep Code
   *  - returns a set of functions : 1 for each lambda parameter
   */
  public static JelinekMercerScorer[] makeFunctions(Parameters p, DocumentOrderedCountIterator it) throws IOException {
    // Extract the set of mus
    String[] lambdaSet = p.get("lambda", "0.5").split(",");
    JelinekMercerScorer[] fns = new JelinekMercerScorer[lambdaSet.length];

    for (int i = 0; i < lambdaSet.length; i++) {
      Parameters fnp = new Parameters();
      fnp.copy(p);
      fnp.set("mu", lambdaSet[i]);
      fns[i] = new JelinekMercerScorer(fnp, it);
    }
    return fns;
  }

  /**
   * Maximize the probability
   * @return
   */
  public double maximumScore() {
      return function.score(1,1);
  }

  public double minimumScore() {
      return function.score(0,1);
  }
}
