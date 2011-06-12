/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.tupleflow.Parameters;

/**
 * This traversal removes extraneous operators:
 * 
 *  #<windowop>:<size>( X ) 
 *    --> X
 * 
 * @author sjh
 */
public class FlatteningTraversal implements Traversal {

  public FlatteningTraversal(Parameters parameters, Retrieval retrieval){
  }

  
  public void beforeNode(Node object) throws Exception {
  }

  public Node afterNode(Node original) throws Exception {

    // if we have a window operator
    if (original.getOperator().equals("ordered")
            || original.getOperator().equals("od")
            || original.getOperator().equals("unordered")
            || original.getOperator().equals("uw")) {
      ArrayList<Node> children = original.getInternalNodes();
      if(children.size() == 1){
        return children.get(0);
      }
    }
    
    // could also flatten combine nodes
    
    return original;
  }
}
