// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.index.TopDocsReader.TopDocument;
import org.galagosearch.core.scoring.JelinekMercerScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength","lambda"})
public class JelinekMercerScoringIterator extends ScoringFunctionIterator {
    protected double loweredMaximum = Double.POSITIVE_INFINITY;


    public JelinekMercerScoringIterator(Parameters p, CountValueIterator it)
            throws IOException {
    super(it, makeFunctions(p, it));
  }

  /**
   * Overriding this in case we're using topdocs, in which case, also drop the
   * maximum once vs multiple times.
   */
  public void setContext(DocumentContext context) {
    if (TopDocsContext.class.isAssignableFrom((context.getClass()))) {
      TopDocsContext tdc = (TopDocsContext) context;
      if (tdc.hold != null) {
        tdc.topdocs.put(this, tdc.hold);
        TopDocument worst = tdc.hold.get(tdc.hold.size()-1);
        loweredMaximum = this.function.score(worst.count, worst.length);
        tdc.hold = null;
      }
    }
    this.context = context;
  }

  /** Parameter Sweep Code
   *  - returns a set of functions : 1 for each lambda parameter
   */
  public static JelinekMercerScorer[] makeFunctions(Parameters p, CountValueIterator it) throws IOException {
    // Extract the set of mus
    String[] lambdaSet = p.get("lambda", "0.5").split(",");
    JelinekMercerScorer[] fns = new JelinekMercerScorer[lambdaSet.length];

    for (int i = 0; i < lambdaSet.length; i++) {
      Parameters fnp = new Parameters();
      fnp.copy(p);
      fnp.set("lambda", lambdaSet[i]);
      //System.err.println("lambda = " + lambdaSet[i]);
      fns[i] = new JelinekMercerScorer(fnp, it);
    }
    return fns;
  }

  /**
   * Maximize the probability
   * @return
   */
  public double maximumScore() {
    if (loweredMaximum != Double.POSITIVE_INFINITY) {
     return loweredMaximum;
    } else {
      return function.score(1,1);
    }
  }

  public double minimumScore() {
      return function.score(0,1);
  }
}
