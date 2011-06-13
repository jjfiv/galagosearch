/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.retrieval.traversal.TransformRootTraversal;
import org.galagosearch.core.retrieval.traversal.BM25RelevanceFeedbackTraversal;
import org.galagosearch.core.retrieval.traversal.FlatteningTraversal;
import org.galagosearch.core.retrieval.traversal.ImplicitFeatureCastTraversal;
import org.galagosearch.core.retrieval.traversal.IndriWindowCompatibilityTraversal;
import org.galagosearch.core.retrieval.traversal.NgramRewriteTraversal;
import org.galagosearch.core.retrieval.traversal.RelevanceModelTraversal;
import org.galagosearch.core.retrieval.traversal.SequentialDependenceTraversal;
import org.galagosearch.core.retrieval.traversal.TextFieldRewriteTraversal;
import org.galagosearch.core.retrieval.traversal.IndriWeightConversionTraversal;
import org.galagosearch.core.retrieval.traversal.InsideToFieldPartTraversal;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author marc
 */
public class CountFeatureFactory extends FeatureFactory {
static String[][] sOperatorLookup = {
    {SynonymIterator.class.getName(), "syn"},
    {SynonymIterator.class.getName(), "synonym"},
    {ExtentInsideIterator.class.getName(), "inside"},
    {OrderedWindowIterator.class.getName(), "ordered"},
    {OrderedWindowIterator.class.getName(), "od"},
    {UnorderedWindowIterator.class.getName(), "unordered"},
    {UnorderedWindowIterator.class.getName(), "uw"},
    {UniversalIndicatorIterator.class.getName(), "all"},
    {ExistentialIndicatorIterator.class.getName(), "any"},
    {BinaryCountIterator.class.getName(), "bcount"}
  };

  // No features here that we know of - cannot produce scores.
  static String[][] sFeatureLookup = {};

  static String[] sTraversalList = {
    InsideToFieldPartTraversal.class.getName(),
    NgramRewriteTraversal.class.getName(),
    IndriWindowCompatibilityTraversal.class.getName(),
    TextFieldRewriteTraversal.class.getName(),
    ImplicitFeatureCastTraversal.class.getName(),
    FlatteningTraversal.class.getName(),
  };

  public CountFeatureFactory(Parameters parameters) {
    super(parameters, sOperatorLookup, sFeatureLookup,
            sTraversalList);
  }
}
