// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.galagosearch.core.retrieval.traversal.PRMSTraversal;
import org.galagosearch.core.retrieval.traversal.AddCombineTraversal;
import org.galagosearch.core.retrieval.traversal.BM25RelevanceFeedbackTraversal;
import org.galagosearch.core.retrieval.traversal.FrequenceFilteringTraversal;
import org.galagosearch.core.retrieval.traversal.ImplicitFeatureCastTraversal;
import org.galagosearch.core.retrieval.traversal.IndriWindowCompatibilityTraversal;
import org.galagosearch.core.retrieval.traversal.NgramRewriteTraversal;
import org.galagosearch.core.retrieval.traversal.SequentialDependenceTraversal;
import org.galagosearch.core.retrieval.traversal.RelevanceModelTraversal;
import org.galagosearch.core.retrieval.traversal.TextFieldRewriteTraversal;
import org.galagosearch.core.retrieval.traversal.WeightConversionTraversal;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;

/**
 * <p>Uses user-provided information to build iterators for retrieval.</p>
 *
 * <p>With no added parameters, this class constructs the basic set of operators
 * in the Galago query language; things like #combine, #inside, and #syn.  However,
 * if you build more operators or traversals, you'll want to provide some
 * parameters to FeatureFactory so it knows where to find your code.</p>
 *
 * <p>If you want to add an operator, try this:</p>
 * <pre>
 *    &gt;operators&lt;
 *        &gt;operator&lt;
 *            &gt;name&lt;myoperator&gt;/name&lt;
 *            &gt;class&lt;org.myorganization.MyOperatorIterator&gt;/class&lt;
 *            &gt;parameters&lt;
 *                &gt;weight&lt;0.5&gt;/weight&lt;
 *            &gt;/parameters&lt;
 *        &gt;/operator&lt;
 *    &gt;/operators&lt;
 * </pre>
 *
 * <p>Once configured like this, you can access your operator in queries as
 * #myoperator().  Galago will instantiate your operator using the class
 * <tt>org.myorganization.MyOperatorIterator</tt>, and it will pass in the
 * argument <tt>weight=0.5</tt> to your operator's parameters object.  Note that
 * the constructor of your operator must start with a Parameters object, and
 * the remaining parameters must be StructuredIterators.  Look at the implementations
 * of the built-in operators for examples.</p>
 *
 * <p>If you want to add some query traversals, try this:</p>
 *
 * <pre>
 *    &gt;traversals&lt;
 *        &gt;traversal&lt;
 *            &gt;class&lt;org.myorganization.MyTraversal&gt;/class&lt;
 *            &gt;order&lt;after&gt;/order&lt;
 *        &gt;/traversal&lt;
 *    &gt;/traversals&lt;
 * </pre>
 *
 * <p>The <tt>order</tt> tag specifies that this traversal should run after
 * all the default traversals.  You could also specify <tt>before</tt> or
 * <tt>instead</tt>.  Order matters in this list of traversals.  The top
 * traversal will be executed first, and the bottom traversal will be executed
 * last.</p>
 *
 * @author trevor, sjh, irmarc
 *
 */
public class DocumentOrderedFeatureFactory extends FeatureFactory {

  // Note that some operators added here are "pseudo-operators" - they don't produce iterators
  // themselves, but they operate as traversals that restructure sub-trees of the query. However
  // these leave some kind of iterable operator in their wake. For example, the "rm" operator is applied
  // after all other transforms to make sure the subquery is properly parameterized, but the rm operator
  // itself is replaced by a combine operator after modification, so from the retrieval's point-of-view,
  // the operator produces an UnfilteredCombinationIterator.
  //
  // -- irmarc
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
    {UnorderedDocumentWindowIterator.class.getName(), "and"},
    {ScaleIterator.class.getName(), "scale"},
    {UnfilteredCombinationIterator.class.getName(), "rm"},
    {UnfilteredCombinationIterator.class.getName(), "seqdep"},
    {UnfilteredCombinationIterator.class.getName(), "bm25rf"},
    {MaxScoreCombinationIterator.class.getName(), "maxscore"}
  };

  static String[][] sFeatureLookup = {
    {DirichletScoringIterator.class.getName(), "dirichlet"},
    {JelinekMercerScoringIterator.class.getName(), "linear"},
    {JelinekMercerScoringIterator.class.getName(), "jm"},
    {BM25ScoringIterator.class.getName(), "bm25"},
    {BM25RFScoringIterator.class.getName(), "bm25rf"},
    {TopDocsScoringIterator.class.getName(), "topdocs"}
  };
  static String[] sTraversalList = {
    SequentialDependenceTraversal.class.getName(),
    PRMSTraversal.class.getName(),
    NgramRewriteTraversal.class.getName(),
    AddCombineTraversal.class.getName(),
    WeightConversionTraversal.class.getName(),
    IndriWindowCompatibilityTraversal.class.getName(),
    TextFieldRewriteTraversal.class.getName(),
    ImplicitFeatureCastTraversal.class.getName(),
    RelevanceModelTraversal.class.getName(),
    BM25RelevanceFeedbackTraversal.class.getName(),
    FrequenceFilteringTraversal.class.getName()
  };

  public DocumentOrderedFeatureFactory(Parameters parameters) {
    operatorLookup = new HashMap<String, OperatorSpec>();
    featureLookup = new HashMap<String, OperatorSpec>();
    this.parameters = parameters;

    for (String[] item : sFeatureLookup) {
      OperatorSpec operator = new OperatorSpec();
      operator.className = item[0];
      String operatorName = item[1];
      featureLookup.put(operatorName, operator);
    }

    for (String[] item : sOperatorLookup) {
      OperatorSpec operator = new OperatorSpec();
      operator.className = item[0];
      String operatorName = item[1];
      operatorLookup.put(operatorName, operator);
    }

    ArrayList<TraversalSpec> afterTraversals = new ArrayList<TraversalSpec>();
    ArrayList<TraversalSpec> beforeTraversals = new ArrayList<TraversalSpec>();
    ArrayList<TraversalSpec> insteadTraversals = new ArrayList<TraversalSpec>();

    for (Value value : parameters.list("traversals/traversal")) {
      String className = value.get("class");
      String order = value.get("order", "after");
      List<Value> params = value.list("parameters");
      if (className == null) {
        throw new RuntimeException("class is required in traversal declarations.");
      }

      TraversalSpec spec = new TraversalSpec();
      spec.className = className;
      if (params != null && params.size() > 0) {
        spec.parameters.copy(new Parameters(params.get(0)));
      }

      if (order.equals("before")) {
        beforeTraversals.add(spec);
      } else if (order.equals("after")) {
        afterTraversals.add(spec);
      } else if (order.equals("instead")) {
        insteadTraversals.add(spec);
      } else {
        throw new RuntimeException("order must be one of {before,after,instead}");
      }
    }

    // If the user doesn't want to replace the current pipeline, add in that pipeline
    if (insteadTraversals.size() == 0) {
      for (String className : sTraversalList) {
        TraversalSpec spec = new TraversalSpec();
        spec.className = className;
        spec.parameters = new Parameters();
        insteadTraversals.add(spec);
      }
    }

    traversals = new ArrayList<TraversalSpec>();
    traversals.addAll(beforeTraversals);
    traversals.addAll(insteadTraversals);
    traversals.addAll(afterTraversals);

    for (Value value : parameters.list("operators/operator")) {
      String className = value.get("class");
      String operatorName = value.get("name");
      List<Value> params = value.list("parameters");
      OperatorSpec spec = new OperatorSpec();

      if (params != null && params.size() > 0) {
        spec.parameters.copy(new Parameters(params.get(0)));
      }

      spec.className = className;
      operatorLookup.put(operatorName, spec);
    }
  }
}
