/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.retrieval.traversal.TransformRootTraversal;
import org.galagosearch.core.retrieval.traversal.BM25RelevanceFeedbackTraversal;
import org.galagosearch.core.retrieval.traversal.FullDependenceTraversal;
import org.galagosearch.core.retrieval.traversal.ImplicitFeatureCastTraversal;
import org.galagosearch.core.retrieval.traversal.IndriWindowCompatibilityTraversal;
import org.galagosearch.core.retrieval.traversal.NgramRewriteTraversal;
import org.galagosearch.core.retrieval.traversal.PRMSTraversal;
import org.galagosearch.core.retrieval.traversal.RelevanceModelTraversal;
import org.galagosearch.core.retrieval.traversal.SequentialDependenceTraversal;
import org.galagosearch.core.retrieval.traversal.TextFieldRewriteTraversal;
import org.galagosearch.core.retrieval.traversal.WeightConversionTraversal;
import org.galagosearch.core.retrieval.traversal.PRMSTraversal;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author marc
 */
public class RankedFeatureFactory extends FeatureFactory {
static String[][] sOperatorLookup = {
    {FilteredCombinationIterator.class.getName(), "filter"},
    {UnfilteredCombinationIterator.class.getName(), "combine"},
    {SynonymIterator.class.getName(), "syn"},
    {SynonymIterator.class.getName(), "synonym"},
    {ExtentInsideIterator.class.getName(), "inside"},
    {OrderedWindowIterator.class.getName(), "ordered"},
    {OrderedWindowIterator.class.getName(), "od"},
    {UnorderedWindowIterator.class.getName(), "unordered"},
    {UnorderedWindowIterator.class.getName(), "uw"},
    {ScaleIterator.class.getName(), "scale"},
    {UnfilteredCombinationIterator.class.getName(), "rm"},
    {UnfilteredCombinationIterator.class.getName(), "bm25rf"},
    {MaxScoreCombinationIterator.class.getName(), "maxscore"},
    {UniversalIndicatorIterator.class.getName(), "all"},
    {ExistentialIndicatorIterator.class.getName(), "any"}
  };
  static String[][] sFeatureLookup = {
    {DirichletScoringIterator.class.getName(), "dirichlet"},
    {JelinekMercerScoringIterator.class.getName(), "linear"},
    {JelinekMercerScoringIterator.class.getName(), "jm"},
    {BM25ScoringIterator.class.getName(), "bm25"},
    {BM25RFScoringIterator.class.getName(), "bm25rf"},
    {BoostingIterator.class.getName(), "boost"}
  };
  static String[] sTraversalList = {
    PRMSTraversal.class.getName(),
    SequentialDependenceTraversal.class.getName(),
    FullDependenceTraversal.class.getName(),
    TransformRootTraversal.class.getName(),
    PRMSTraversal.class.getName(),
    NgramRewriteTraversal.class.getName(),
    WeightConversionTraversal.class.getName(),
    IndriWindowCompatibilityTraversal.class.getName(),
    TextFieldRewriteTraversal.class.getName(),
    ImplicitFeatureCastTraversal.class.getName(),
    RelevanceModelTraversal.class.getName(),
    BM25RelevanceFeedbackTraversal.class.getName()
  };

  public RankedFeatureFactory(Parameters parameters) {
    super(parameters, sOperatorLookup, sFeatureLookup,
            sTraversalList);
  }
}
