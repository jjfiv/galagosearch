// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;

/**
 *
 * @author sjh
 */
@RequiredStatistics(statistics = {"freq", "freqop"})
public class FrequenceFilteringTraversal implements Traversal {

  int threshold;
  HashSet<String> whiteList = new HashSet();

  public FrequenceFilteringTraversal(Parameters parameters, Retrieval retrieval) {
    threshold = (int) parameters.get("freq", 0);
    List<Value> fops = parameters.list("freqop");
    if(fops.size() > 0){
      for(Value v: fops ){
        whiteList.add( v.toString() );
      }
    }

    System.err.println(whiteList.size());
    for(String w : whiteList){
      System.err.println(w);
    }
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

      if ((children.size() > 1)
              || (children.get(0).getOperator().equals("freq"))) {
        return node;
      }


      if ((whiteList.size() > 0)
              && (! whiteList.contains(children.get(0).getOperator()))) {
        return node;
      }

      Node newChild = new Node("freq", Integer.toString(threshold), children);
      ArrayList<Node> newChildren = new ArrayList();
      newChildren.add(newChild);

      Node newNode = new Node(node.getOperator(), node.getParameters(), newChildren, 0);

      return newNode;
    } else {
      return node;
    }
  }
}
