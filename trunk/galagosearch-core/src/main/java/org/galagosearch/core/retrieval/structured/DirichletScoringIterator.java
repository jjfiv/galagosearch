/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.scoring.DirichletScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * A ScoringIterator that makes use of the DirichletScorer function
 * for converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength"})
public class DirichletScoringIterator extends ScoringFunctionIterator {
  public DirichletScoringIterator(Parameters p, DocumentOrderedCountIterator it)
          throws IOException {
    super(it, makeFunctions(p, it));
  }

  public static DirichletScorer[] makeFunctions(Parameters p, DocumentOrderedCountIterator it) throws IOException {
    // Extract the set of mus
    String[] muSet = p.get("mu", "1500").split(",");
    DirichletScorer[] fns = new DirichletScorer[muSet.length];

    for (int i = 0; i < muSet.length; i++) {
      Parameters fnp = new Parameters();
      fnp.copy(p);
      fnp.set("mu", muSet[i]);
      fns[i] = new DirichletScorer(fnp, it);
    }
    return fns;
  }
}
