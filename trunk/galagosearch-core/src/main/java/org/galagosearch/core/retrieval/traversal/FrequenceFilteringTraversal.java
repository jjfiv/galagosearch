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
  Retrieval retrieval;


  public FrequenceFilteringTraversal(Parameters parameters, Retrieval retrieval) {
    threshold = (int) parameters.get("freq", 0);
    this.retrieval = retrieval;

    List<Value> fops = parameters.list("freqop");
    if(fops.size() > 0){
      for(Value v: fops ){
        whiteList.add( v.toString() );
      }
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

      if (children.size() > 1){
        return node;
      }

      Node child = children.get(0);

      if ((whiteList.size() > 0)
              && (! whiteList.contains(child.getOperator()))) {
        return node;
      }

      long count = retrieval.xcount(child);
      if(count < threshold){
        // this term can not exist in the index
        child.getParameters().set("default", "!");
      }

      return node;
    } else {
      return node;
    }
  }
}
