// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
public abstract class ScoreCombinationIterator extends DocumentOrderedScoreIterator {

  double[] weights;
  double weightSum;
  DocumentOrderedScoreIterator[] iterators;
  boolean done;
  // parameter sweep stuff
  double[][] weightLists = null; // double[parameterID][nodeID]
  double[] weightSums = null; // double[parameterID]
  String[] parameterStrings = null; // String[parameterID]

  public ScoreCombinationIterator(Parameters parameters,
          DocumentOrderedScoreIterator[] childIterators) {

    weights = new double[childIterators.length];
    weightSum = 0.0;
    int parameterSetSize = 1;

    for (int i = 0; i < weights.length; i++) {
      String[] weightStrings = parameters.get(Integer.toString(i), "1.0").split(",");
      weights[i] = Double.parseDouble(weightStrings[0]);
      weightSum += weights[i];

      // check if we need to initialize our arrays
      if (weightLists == null) {
        parameterSetSize = weightStrings.length;
        weightLists = new double[parameterSetSize][childIterators.length];
        weightSums = new double[parameterSetSize];
        parameterStrings = new String[parameterSetSize];
        Arrays.fill(weightSums, 0.0);
        Arrays.fill(parameterStrings, "#combine");
      }
      assert parameterSetSize == weightStrings.length : "COMBINE NODE ERROR : all weight lists need to be the same size";

      for (int j = 0; j < parameterSetSize; j++) {
        weightLists[j][i] = Double.parseDouble(weightStrings[j]);
        weightSums[j] += weightLists[j][i];
        parameterStrings[j] += ":" + i + "=" + weightLists[j][i];
      }
    }

    this.iterators = childIterators;
  }

  public double score() {
    double total = 0;

    for (int i = 0; i < iterators.length; i++) {
      total += weights[i] * iterators[i].score();
    }
    return total / weightSum;
  }

  public void movePast(int document) throws IOException {
    for (DocumentOrderedIterator iterator : iterators) {
      iterator.movePast(document);
    }
  }

  public void moveTo(int document) throws IOException {
    for (DocumentOrderedIterator iterator : iterators) {
      iterator.moveTo(document);
    }
  }

  public boolean skipToDocument(int document) throws IOException {
    boolean skipped = true;
    for (DocumentOrderedIterator iterator : iterators) {
      skipped = skipped && iterator.skipToDocument(document);
    }
    return skipped;
  }

  public void reset() throws IOException {
    for (DocumentOrderedIterator iterator : iterators) {
      iterator.reset();
    }
  }

  //***********************//
  //  parameter sweep code //
  //***********************//
  public Map<String, Double> parameterSweepScore() {
    // first get all of the child scores - there are some cases to consider
    // map(child id -> scoreMap(paramString -> score))
    Map<String, Double>[] childScoreMaps = new Map[iterators.length];
    for (int i = 0; i < iterators.length; i++) {
      childScoreMaps[i] = iterators[i].parameterSweepScore();
    }

    // special case : parallel child lists of smoothing nodes
    //  - each child has returned a matching set of scores
    //  - eg: each child has been dirichlet smoothed (where each smoother used the same set of mus)
    if (checkForParallelSmoothingScoreMaps(childScoreMaps)) {
      HashMap<String, Double> results = new HashMap();

      // for each set of combination weights
      for (int j = 0 ; j < weightLists.length ; j++ ) {
        // for each aligned child parameter compute the weighted combination
        for (String childParameters : childScoreMaps[0].keySet()) {
          String resultParameters = parameterStrings[j] + "(";
          double total = 0.0;
          for (int i = 0; i < iterators.length; i++) {
          resultParameters += " " + childParameters ;
            total += weightLists[j][i] * childScoreMaps[i].get(childParameters);
          }
          resultParameters += " )";
          results.put(resultParameters, total / weightSums[j]);
        }
      }

      return results;
    }

    // more common case: non-matching children
    //  - eg: #combine( w1 #combine( 3 children ) w2 #combine( 2 children ) )
    //  - this option tries to do the complete cross product of the children
    //  + with the parameter sweep of the current node
    //  << could be overkill for some uses - but will ensure the desired sweeps are a subset of the results >>
    HashMap<String, Double> results = new HashMap();

    // for each set of combination weights

    for (int j = 0 ; j < weightLists.length ; j++ ) {

      // do a cross-product of child parameters
      ArrayList<Double> totals = new ArrayList();
      totals.add(0.0);
      ArrayList<String> resultParameters = new ArrayList();
      resultParameters.add(parameterStrings[j] + "(");

      for (int i = 0; i < iterators.length; i++) {
        // prepare the new list of totals and parameter strings
        ArrayList<Double> newTotals = new ArrayList();
        ArrayList<String> newParameterStrings = new ArrayList();
        
        for (String childParameters : childScoreMaps[i].keySet()) {
          for (int t = 0; t < totals.size(); t++) {
            newTotals.add(totals.get(t) + weightLists[j][i] * childScoreMaps[i].get(childParameters));
            newParameterStrings.add(resultParameters.get(t) + " " + childParameters);
          }
        }
        // replace old totals with new totals
        totals = newTotals;
        resultParameters = newParameterStrings;
      }

      // now we have a full set of totals + parameterStrings
      for (int t = 0; t < totals.size(); t++) {
        results.put((resultParameters.get(t) + " )"), (totals.get(t) / weightSums[j]));
      }
    }
    return results;
  }

  private boolean checkForParallelSmoothingScoreMaps(Map<String, Double>[] childScoreMaps) {
    int maxChildSetSize = 0;
    Set<String> intersection = null;
    for (Map<String, Double> childScoreSet : childScoreMaps) {
      if (intersection == null) {
        intersection = new HashSet(childScoreSet.keySet());
      } else {
        intersection.retainAll(childScoreSet.keySet());
      }
      maxChildSetSize = Math.max(maxChildSetSize, childScoreSet.size());
    }

    // if the intersection is not the same size as the largest child set
    //  then we do not have parallel child maps
    if (intersection.size() != maxChildSetSize)
      return false;

    // if any string contains a '#' then it was created by an operator not a smoothing node
    for(String parameter : intersection){
      if(parameter.contains("#"))
        return false;
    }
    return true;
  }
}


