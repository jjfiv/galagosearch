// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.CountIterator;
import org.galagosearch.core.retrieval.structured.ScoreIterator;
import org.galagosearch.core.retrieval.structured.ScoringFunctionIterator;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;

/**
 * For many kinds of queries, it may be preferable to not have to type
 * an explicit #feature operator around a count or extents term.  For example,
 * we want #combine(#feature:dirichlet(#counts:dog())) to be the same as
 * #combine(dog).  This transformation automatically adds the #feature:dirichlet
 * operator.
 * 
 * (12/21/2010, irmarc): Modified to annotate topdocs feature nodes, as well as generate them
 *                        if specified by construction parameters.
 *
 * @author trevor, irmarc
 */
@RequiredStatistics(statistics = {"retrievalGroup", "scorer", "mu", "lambda", "mod"})
public class ImplicitFeatureCastTraversal implements Traversal {

  Retrieval retrieval;
  String retrievalGroup;
  Parameters parameters;

  public ImplicitFeatureCastTraversal(Parameters parameters, Retrieval retrieval) {
    this.retrieval = retrieval;
    this.retrievalGroup = parameters.get("retrievalGroup");
    this.parameters = parameters;
  }

  Node createSmoothingNode(Node child) throws Exception {

    /** Check if the child is an 'extents' node
     *    If so - we can replace extents with counts.
     *    This can lead to performance improvements within positions indexes
     *    as the positional data does NOT need to be read for the feature scorer to operate.
     */
    if (child.getOperator().equals("extents")) {
      child = new Node("counts", child.getParameters(), child.getInternalNodes(), child.getPosition());
    }

    ArrayList<Node> data = new ArrayList<Node>();
    data.add(child);
    String scorerType = parameters.get("scorer", "dirichlet");
    Node smoothed = new Node("feature", scorerType, data, child.getPosition());
    // TODO - add in smoothing parameters, modifiers
    Parameters p = smoothed.getParameters();
    if (parameters.containsKey("mod")) {
      p.add("mod", parameters.get("mod"));
    }

    if (!parameters.get("topdocs", false)) {
      return smoothed;
    }

    // If we're here, we should be adding a topdocs node
    return createTopdocsNode(smoothed);
  }

  Node createTopdocsNode(Node child) throws Exception {
    // First (and only) child should be a scoring function iterator node
    if (!isScoringFunctionNode(child)) {
      return child;
    }

    // The replacement
    ArrayList<Node> children = new ArrayList<Node>();
    children.add(child);
    Node workingNode = new Node("feature", "topdocs", children, child.getPosition());

    // count node, with the information we need
    Node grandchild = child.getInternalNodes().get(0);
    Parameters descendantParameters = grandchild.getParameters();
    Parameters workingParameters = workingNode.getParameters();
    workingParameters.set("term", descendantParameters.get("default"));
    workingParameters.set("loc", descendantParameters.get("part"));
    workingParameters.set("index", parameters.get("index"));
    return workingNode;
  }

  public boolean isCountNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node, retrievalGroup);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return CountIterator.class.isAssignableFrom(outputClass);
  }

  public boolean isScoringFunctionNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node, retrievalGroup);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return ScoringFunctionIterator.class.isAssignableFrom(outputClass);
  }

  public void beforeNode(Node node) throws Exception {
  }

  public Node afterNode(Node node) throws Exception {
    // Determine if we need to add a scoring node
    Node scored = addScorers(node);
    return scored;
  }

  public Node addScorers(Node node) throws Exception {
    ArrayList<Node> newChildren = new ArrayList<Node>();

    NodeType nodeType = retrieval.getNodeType(node, retrievalGroup);
    if (nodeType == null) {
      return node;
    }

    ArrayList<Node> children = node.getInternalNodes();
    // Given that we're going to pass children.size() + 1 parameters to
    // this constructor, what types should those parameters have?
    Class[] types = nodeType.getParameterTypes(children.size() + 1);
    if (types == null) {
      return node;
    }

    for (int i = 1; i < types.length; ++i) {
      Node child = children.get(i - 1);
      // If the parent will expect a ScoreIterator at this position, but
      // we've got a CountIterator here, we'll perform a conversion step.
      if (ScoreIterator.class.isAssignableFrom(types[i])
              && isCountNode(children.get(i - 1))) {
        Node feature = createSmoothingNode(child);
        newChildren.add(feature);
      } else {
        newChildren.add(child);
      }
    }

    return new Node(node.getOperator(), node.getParameters(),
            newChildren, node.getPosition());
  }
}
