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
    super(it, new JelinekMercerScorer(p, it));
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
