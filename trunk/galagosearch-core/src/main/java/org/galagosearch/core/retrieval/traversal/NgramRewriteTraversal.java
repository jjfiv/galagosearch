// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;

import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.tupleflow.Parameters;
import org.tartarus.snowball.ext.englishStemmer;

/*
 * Ngram operator
 * 
 * rewrites the ngram meta-operator :
 * #ngram( term term term )
 * to an operator that can be run on an n-gram index :
 * #extent:part=3-gram-2-count:default=term~term~term~()
 * 
 * @author sjh
 * 
 */
public class NgramRewriteTraversal implements Traversal {
  private StructuredIndex index;

  public NgramRewriteTraversal(Parameters parameters, StructuredRetrieval retrieval) {
      this.index = retrieval.getIndex();
  }

  /*
   * before node checks that an ngram operator is possible
   * 
   */
  public void beforeNode(Node node) throws Exception {
    if(node.getOperator().equals("ngram")){
      for(Node child: node.getInternalNodes()){
        if( ! child.getOperator().equals("text") ){
          throw new Exception("Arguments of an ngram operator need to be text nodes.\n" +
              "print out of the problematic argument : \n" + child.toString() );
        }
      }

      int n = node.getInternalNodes().size();
      if( ! possibleIndexExists(n)){
        throw new Exception("No indexes found for "+n+"-grams. Aborting.\n"+node.toString());
      }
    }
  }

  /*
   * after node creates the extent node that accesses the correct ngram index
   * 
   */
  public Node afterNode(Node original) throws Exception {
      
    if(original.getOperator().equals("ngram")){
      Parameters p = original.getParameters();
      if( ! p.containsKey("part") ){
        int n = original.getInternalNodes().size();
        String part = getnGramPartName(n);
        System.err.println("Using part:" + part);
        p.add("part", part);
      } 

      StringBuilder sb = new StringBuilder();
      for(Node child : original.getInternalNodes()){
        sb.append(child.getDefaultParameter());
        sb.append("~");
      }
      p.set("default", sb.toString());
      return new Node("extents", p, new ArrayList<Node>(), original.getPosition());
    }
    return original;
  }
  

  // minimum work to ensure that an n-gram index exists
  private boolean possibleIndexExists(int n){
    for(String part : index.getPartNames()){
      if(part.startsWith(Integer.toString(n) + "-grams-"))
        return true;
    }
    return false;
  }

  // will pick index part with the lowest threshold
  // n-gram indexes look like: n-gram-h-count
  private String getnGramPartName(int n){
    String selectedPart = null;
    int selectedH = Integer.MAX_VALUE;

    for(String part : index.getPartNames()){
      if(part.startsWith(Integer.toString(n) + "-grams-") &&
          (! part.contains("-stemmed"))){

        int h = Integer.parseInt(part.split("-")[2]);
        if(h < selectedH){
          selectedH = h;
          selectedPart = part;
        }
      }
    }
    return selectedPart;
  }
}
