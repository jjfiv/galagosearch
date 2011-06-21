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
    if(original.getOperator().equals("combine")){
      ArrayList<Node> children = original.getInternalNodes();
      Parameters oldParameters = original.getParameters();

      boolean nestedCombine = false;
      ArrayList<Node> newChildren = new ArrayList();
      Parameters newParameters = new Parameters();

      
      for( int i=0; i < children.size() ; i++ ){
        Node child = children.get(i);
        // if we have a nested combine - collect sub children
        if(child.getOperator().equals("combine")){
          nestedCombine = true;
          ArrayList<Node> subChildren = child.getInternalNodes();
          double weightSum = 0.0;
          for(int j=0 ; j < subChildren.size(); j++){
            weightSum += child.getParameters().get(Integer.toString(j), 1.0);
          }
          for(int j=0 ; j < subChildren.size(); j++){
            Node subChild = subChildren.get(j);
            double normWeight = child.getParameters().get(Integer.toString(j), 1.0) / weightSum;
            double newWeight = oldParameters.get(Integer.toString(i), 1.0) * normWeight;
            newParameters.add( Integer.toString(newChildren.size()), Double.toString(newWeight) );
            newChildren.add(subChild);
          }
          
        // otherwise we have a normal child
        } else {
          // newChildren.size == the new index of this child
          newParameters.add( Integer.toString(newChildren.size()) , oldParameters.get(Integer.toString(i), "1.0"));
          newChildren.add( child );
        }
      }
      
      if( nestedCombine ){
        return new Node("combine", newParameters, newChildren, original.getPosition());
      }
    }
    
    return original;
  }
}
