// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
@RequiredStatistics(statistics = {"freq"})
public class FrequenceFilteringTraversal implements Traversal {

  int threshold;

  public FrequenceFilteringTraversal(Parameters parameters, Retrieval retrieval) {
    threshold = (int) parameters.get("freq", 0);
  }

  public void beforeNode(Node object) throws Exception {
    //pass
  }

  public Node afterNode(Node node) throws Exception {
    if (threshold == 0) {
      return node;
    }

    // otherwise
    if (node.getOperator().equals("feature")) {
      ArrayList<Node> children = node.getInternalNodes(); // should only be one

      if((children.size() > 1) ||
         // (children.get(0).getOperator().equals("extents")) ||
         (children.get(0).getOperator().equals("freq"))) {
        return node;

      } else {

        Node newChild = new Node("freq", Integer.toString(threshold), children);
        ArrayList<Node> newChildren = new ArrayList();
        newChildren.add(newChild);

        Node newNode = new Node(node.getOperator(), node.getParameters(), newChildren, 0);

        return newNode;
      }
    } else {
      return node;
    }
  }
}

