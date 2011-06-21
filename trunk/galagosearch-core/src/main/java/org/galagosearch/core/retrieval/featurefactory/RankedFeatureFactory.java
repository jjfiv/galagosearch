/*
 * BSD License (http://www.galagosearch.org/license)
 */

package org.galagosearch.core.retrieval.featurefactory;

import org.galagosearch.core.retrieval.structured.BM25RFScoringIterator;
import org.galagosearch.core.retrieval.structured.BM25ScoringIterator;
import org.galagosearch.core.retrieval.structured.BoostingIterator;
import org.galagosearch.core.retrieval.structured.DirichletScoringIterator;
import org.galagosearch.core.retrieval.structured.ExistentialIndicatorIterator;
import org.galagosearch.core.retrieval.structured.ExtentInsideIterator;
import org.galagosearch.core.retrieval.structured.FilteredIterator;
import org.galagosearch.core.retrieval.structured.JelinekMercerScoringIterator;
import org.galagosearch.core.retrieval.structured.MaxScoreCombinationIterator;
import org.galagosearch.core.retrieval.structured.OrderedWindowIterator;
import org.galagosearch.core.retrieval.structured.ScaleIterator;
import org.galagosearch.core.retrieval.structured.SynonymIterator;
import org.galagosearch.core.retrieval.structured.ThresholdIterator;
import org.galagosearch.core.retrieval.structured.UnfilteredCombinationIterator;
import org.galagosearch.core.retrieval.structured.UniversalIndicatorIterator;
import org.galagosearch.core.retrieval.structured.UnorderedWindowIterator;
import org.galagosearch.core.retrieval.traversal.TransformRootTraversal;
import org.galagosearch.core.retrieval.traversal.BM25RelevanceFeedbackTraversal;
import org.galagosearch.core.retrieval.traversal.FlatteningTraversal;
import org.galagosearch.core.retrieval.traversal.FullDependenceTraversal;
import org.galagosearch.core.retrieval.traversal.ImplicitFeatureCastTraversal;
import org.galagosearch.core.retrieval.traversal.IndriWindowCompatibilityTraversal;
import org.galagosearch.core.retrieval.traversal.InsideToFieldPartTraversal;
import org.galagosearch.core.retrieval.traversal.NgramRewriteTraversal;
import org.galagosearch.core.retrieval.traversal.RelevanceModelTraversal;
import org.galagosearch.core.retrieval.traversal.SequentialDependenceTraversal;
import org.galagosearch.core.retrieval.traversal.TextFieldRewriteTraversal;
import org.galagosearch.core.retrieval.traversal.IndriWeightConversionTraversal;
import org.galagosearch.core.retrieval.traversal.PRMSTraversal;
import org.galagosearch.core.retrieval.traversal.RemoveStopwordsTraversal;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author marc
 */
public class RankedFeatureFactory extends FeatureFactory {
  static String[][] sOperatorLookup = {
    {FilteredIterator.class.getName(), "filter"},
    {ThresholdIterator.class.getName(), "threshold"},
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
    RemoveStopwordsTraversal.class.getName(),
    SequentialDependenceTraversal.class.getName(),
    FullDependenceTraversal.class.getName(),
    TransformRootTraversal.class.getName(),
    PRMSTraversal.class.getName(),
    InsideToFieldPartTraversal.class.getName(),
    NgramRewriteTraversal.class.getName(),
    IndriWeightConversionTraversal.class.getName(),
    IndriWindowCompatibilityTraversal.class.getName(),
    TextFieldRewriteTraversal.class.getName(),
    ImplicitFeatureCastTraversal.class.getName(),
    FlatteningTraversal.class.getName(),
    RelevanceModelTraversal.class.getName(),
    BM25RelevanceFeedbackTraversal.class.getName()
  };

  public RankedFeatureFactory(Parameters parameters) {
    super(parameters, sOperatorLookup, sFeatureLookup,
            sTraversalList);
  }
}
