// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectProcedure;
import java.io.IOException;
import java.util.Arrays;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 * [sjh]: modified to scale the child nodes acording to weights in the parameters object
 * - this fixes hierarchical scaling problems by normalizing the node
 *
 * [irmarc]: Part of a refactor - this node now represents a score iterator that navigates
 *          via document-ordered methods
 *
 * @author trevor, sjh, irmarc
 */
public abstract class ScoreCombinationIterator implements ScoreValueIterator {

  protected double[] weights;
  protected double weightSum;
  protected ScoreValueIterator[] iterators;
  protected boolean done;
  // parameter sweep stuff
  protected double[][] weightLists = null; // double[parameterID][nodeID]
  protected double[] weightSums = null; // double[parameterID]
  protected String[] parameterStrings = null; // String[parameterID]

  public ScoreCombinationIterator(Parameters parameters,
          ScoreValueIterator[] childIterators) {

    weights = new double[childIterators.length];
    weightSum = 0.0;
    int parameterSetSize = 1;
    String[] weightStrings = parameters.get(Integer.toString(0), "1.0").split(",");
    // check if we need to initialize our arrays
    parameterSetSize = weightStrings.length;
    weightLists = new double[parameterSetSize][childIterators.length];
    weightSums = new double[parameterSetSize];
    parameterStrings = new String[parameterSetSize];
    StringBuilder[] parameterStringBuilders = new StringBuilder[parameterSetSize];
    Arrays.fill(weightSums, 0.0);

    for (int i = 0; i < weights.length; i++) {
      weightStrings = parameters.get(Integer.toString(i), "1.0").split(",");
      weights[i] = Double.parseDouble(weightStrings[0]);
      weightSum += weights[i];

      assert parameterSetSize == weightStrings.length : "COMBINE NODE ERROR : all weight lists need to be the same size";

      for (int j = 0; j < parameterSetSize; j++) {
        weightLists[j][i] = Double.parseDouble(weightStrings[j]);
        weightSums[j] += weightLists[j][i];
        if (parameterStringBuilders[j] == null) {
          parameterStringBuilders[j] = new StringBuilder("#combine");
        }
        parameterStringBuilders[j].append(":").append(i).append("=").append(weightLists[j][i]);
      }
    }

    for (int j = 0; j < parameterSetSize; j++) {
      parameterStrings[j] = parameterStringBuilders[j].toString();
    }

    this.iterators = childIterators;
  }

  public void setContext(DocumentContext context) {
    for (ScoreIterator iterator : iterators) {
      iterator.setContext(context);
    }
  }

  public DocumentContext getContext() {
    return iterators[0].getContext();
  }

  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Score combine nodes don't have singular values");
  }

  public int compareTo(ValueIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return currentCandidate() - other.currentCandidate();
  }

  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  public boolean moveTo(int identifier) throws IOException {
    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        iterator.moveTo(identifier);
      }
    }
    return hasMatch(identifier);
  }

  public double score() {
    double total = 0;

    for (int i = 0; i < iterators.length; i++) {
      double score = iterators[i].score();
      total += weights[i] * score;
    }
    return total / weightSum;
  }

  public double score(DocumentContext dc) {
    double total = 0;

    for (int i = 0; i < iterators.length; i++) {
      double score = iterators[i].score(dc);
      total += weights[i] * score;
    }
    return total / weightSum;
  }

  public boolean isDone() {
    return done;
  }

  public void reset() throws IOException {
    for (StructuredIterator iterator : iterators) {
      iterator.reset();
    }
  }

  public double minimumScore() {
    double min = 0;
    for (int i = 0; i < iterators.length; i++) {
      min += weights[i] * iterators[i].minimumScore();
    }
    return (min / weightSum);
  }

  public double maximumScore() {
    double max = 0;
    for (int i = 0; i < iterators.length; i++) {
      max += weights[i] * iterators[i].maximumScore();
    }
    return (max / weightSum);
  }

  //***********************//
  //  parameter sweep code //
  //***********************//
  public TObjectDoubleHashMap<String> parameterSweepScore() {
    // first get all of the child scores - there are some cases to consider
    // map(child id -> scoreMap(paramString -> score))
    final TObjectDoubleHashMap<String>[] childScoreMaps = new TObjectDoubleHashMap[iterators.length];
    for (int i = 0; i < iterators.length; i++) {
      childScoreMaps[i] = iterators[i].parameterSweepScore();
    }

    // special case : parallel child lists of smoothing nodes
    //  - each child has returned a matching set of scores
    //  - eg: each child has been dirichlet smoothed (where each smoother used the same set of mus)
    if (checkForParallelSmoothingScoreMaps(childScoreMaps)) {
      final TObjectDoubleHashMap<String> results = new TObjectDoubleHashMap();

      // for each set of combination weights
      for (int j = 0; j < weightLists.length; j++) {
        StringBuilder sb = new StringBuilder(parameterStrings[j]).append("(");
        final String prefix = sb.toString();
        final int fj = j;
        // for each aligned child parameter compute the weighted combination
        childScoreMaps[0].forEachKey(new TObjectProcedure<String>() {

          public boolean execute(String childParameters) {
            StringBuilder sbclone = new StringBuilder(prefix);
            double total = 0.0;
            for (int i = 0; i < iterators.length; i++) {
              sbclone.append(" ").append(childParameters);
              total += weightLists[fj][i] * childScoreMaps[i].get(childParameters);
            }
            results.put(sbclone.append(" )").toString(), total / weightSums[fj]);
            return true;
          }
        });
      }

      return results;
    }

    // more common case: non-matching children
    //  - eg: #combine( w1 #combine( 3 children ) w2 #combine( 2 children ) )
    //  - this option tries to do the complete cross product of the children
    //     with the parameter sweep of the current node
    //  << could be overkill for some uses - but will ensure the desired sweep is a subset of the results >>
    TObjectDoubleHashMap<String> results = new TObjectDoubleHashMap();

    // for each set of combination weights
    for (int j = 0; j < weightLists.length; j++) {

      // do a cross-product of child parameters
      double[] totals = new double[1];
      totals[0] = 0.0;
      StringBuilder[] resultsParameters = new StringBuilder[1];
      resultsParameters[0] = new StringBuilder(parameterStrings[j]).append("(");

      for (int i = 0; i < iterators.length; i++) {
        // prepare the new list of totals and parameter strings
        double[] newTotals = new double[totals.length * childScoreMaps[i].size()];
        StringBuilder[] newParameterStrings = new StringBuilder[newTotals.length];
        int index = 0;

        for (String childParameters : childScoreMaps[i].keys(new String[0])) {
          for (int t = 0; t < totals.length; t++) {
            newTotals[index] = totals[t] + (weightLists[j][i] * childScoreMaps[i].get(childParameters));
            newParameterStrings[index] = new StringBuilder(resultsParameters[t]).append(" ").append(childParameters);
            index++;
          }
        }
        // replace old totals with new totals
        totals = newTotals;
        resultsParameters = newParameterStrings;
      }

      // now we have a full set of totals + parameterStrings
      for (int t = 0; t < totals.length; t++) {
        results.put((resultsParameters[t].append(" )").toString()), (totals[t] / weightSums[j]));
      }
    }
    return results;
  }

  private boolean checkForParallelSmoothingScoreMaps(final TObjectDoubleHashMap<String>[] childScoreMaps) {
    int childSize = childScoreMaps[0].size();
    for (int i = 1; i < childScoreMaps.length; i++) {
      if (childSize != childScoreMaps[i].size()) {
        return false;
      }
    }

    if (!childScoreMaps[0].forEachKey(new TObjectProcedure<String>() {

      public boolean execute(String k) {
        for (int i = 1; i < childScoreMaps.length; i++) {
          // if any string contains a '#' then it was created by an operator not a smoothing node
          if (k.contains("#")) {
            return false;
          }
          if (!childScoreMaps[i].containsKey(k)) {
            return false;
          }
        }
        return true;
      }
    })) {
      return false;
    }
    return true;
  }
}
