package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.scoring.BM25RFScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 * Implements an iterator over the BM25RF scoring mechanism based on TSV (term-selection value) fitness.
 * This comes from some work by Stephen Robertson and others, I believe in TREC 2004 or so.
 * Obviously, don't quote me on that. A decent review of the original TSV algorithm was done by
 * Billerbeck and Zobel in using short document summaries for fast query expansion.
 * 
 * @author irmarc
 */
@RequiredStatistics(statistics = {"documentCount"})
public class BM25RFScoringIterator extends ScoringFunctionIterator {

  public BM25RFScoringIterator(Parameters p, CountValueIterator it)
          throws IOException {
    super(it, new BM25RFScorer(p, it));
  }

  /**
   * We override the score method here b/c the superclass version will always
   * call score, but with a 0 count, in case the scorer smoothes. In this case,
   * the count and length are irrelevant, and it's matching on the identifier
   * list that matters.
   *
   * @return
   */
  @Override
  public double score() {
    if (iterator.currentCandidate() == context.document) {
      return function.score(((CountIterator) iterator).count(), context.length);
    } else {
      return 0;
    }
  }

  /**
   * For this particular scoring function, the parameters are irrelevant.
   * Always returns the predetermined boosting score.
   * @return
   */
  public double maximumScore() {
    return function.score(0, 0);
  }

  /**
   * For this particular scoring function, the parameters are irrelevant
   * Always returns the predetermined boosting score.
   * @return
   */
  public double minimumScore() {
    return function.score(0, 0);
  }
}
