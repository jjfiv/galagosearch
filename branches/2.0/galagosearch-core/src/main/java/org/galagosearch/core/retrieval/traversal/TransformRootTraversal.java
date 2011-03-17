// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.CountIterator;
import org.galagosearch.core.retrieval.structured.IndicatorIterator;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.retrieval.structured.ScoreIterator;
import org.galagosearch.core.retrieval.structured.ScoringFunctionIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 * StructuredQuery puts a "root" operator at the top of the query tree. We now have to
 * make that node have a proper operator given the processing context (i.e. parameters)
 * @author trevor
 * @author irmarc
 *
 */
@RequiredStatistics(statistics = {"queryType", "retrievalGroup"})
public class TransformRootTraversal implements Traversal {

  int levels = 0;
  String queryType;
  Retrieval retrieval;
  String retrievalGroup;

  public TransformRootTraversal(Parameters parameters, Retrieval retrieval) {
    queryType = parameters.get("queryType", "ranked");
    this.retrieval = retrieval;
    this.retrievalGroup = parameters.get("retrievalGroup");
  }

  public void beforeNode(Node object) throws Exception {
    levels++;
  }

  public Node afterNode(Node original) throws Exception {
    levels--;
    if (levels > 0) {
      return original;
    } else {
      // We need to determine what's under us to know if we can
      // just echo it up. Do case by case.
      if (queryType.equals("boolean")) {
        return transformBooleanRoot(original);
      } else if (queryType.equals("count")) {
        return transformCountRoot(original);
      } else {
        return transformRankedRoot(original);
      }
    }
  }

  private Node transformBooleanRoot(Node root) throws Exception {
    if (root.getOperator().equals("root")) {
      return new Node("any", root.getParameters(), root.getInternalNodes(), root.getPosition());
    }

    // We had a singleton - if it's legit for this query type,
    // just return it. Otherwise wrap it.
    if (isIndicatorNode(root)) {
      return root;
    }
    ArrayList<Node> children = new ArrayList<Node>();
    children.add(root);
    return new Node("any", new Parameters(), children, root.getPosition());
  }

  private Node transformCountRoot(Node root) throws Exception {
    if (root.getOperator().equals("root")) {
      return new Node("synonym", root.getParameters(), root.getInternalNodes(), root.getPosition());
    }

    if (isCountNode(root)) {
      return root;
    }

    ArrayList<Node> children = new ArrayList<Node>();
    children.add(root);
    return new Node("synonym", new Parameters(), children, root.getPosition());
  }

  private Node transformRankedRoot(Node root) throws Exception {
    if (root.getOperator().equals("root")) {
      return new Node("combine", root.getParameters(), root.getInternalNodes(), root.getPosition());
    }

    if (isScoreNode(root)) {
      return root;
    }

    ArrayList<Node> children = new ArrayList<Node>();
    children.add(root);
    return new Node("combine", new Parameters(), children, root.getPosition());
  }

  public boolean isCountNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node, retrievalGroup);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return CountIterator.class.isAssignableFrom(outputClass);
  }

  public boolean isScoreNode(Node node) throws Exception {
    NodeType nodeType;
    try {
      nodeType = retrieval.getNodeType(node, retrievalGroup);
    } catch (Exception e) {
      return false;
    }
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return ScoreIterator.class.isAssignableFrom(outputClass);
  }

  public boolean isScoringFunctionNode(Node node) throws Exception {
    NodeType nodeType;
    try {
      nodeType = retrieval.getNodeType(node, retrievalGroup);
    } catch (Exception e) {
      return false;
    }

    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return ScoringFunctionIterator.class.isAssignableFrom(outputClass);
  }

  public boolean isIndicatorNode(Node node) throws Exception {
    NodeType nodeType;
    try {
      nodeType = retrieval.getNodeType(node, retrievalGroup);
    } catch (Exception e) {
      return false;
    }
    nodeType = retrieval.getNodeType(node, retrievalGroup);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return IndicatorIterator.class.isAssignableFrom(outputClass);
  }
}
