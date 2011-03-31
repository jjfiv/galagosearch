// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.tupleflow.Parameters;

/**
 * Transforms a #prms operator into a full expansion of the
 * PRM-S model. That means:
 *
 * Given `meg ryan war`, the output should be like:
 *
 * #combine(
 * #combine:0=0.407:1=0.382:2=0.187 ( meg.cast  meg.team  meg.title )
 * #combine:0=0.601:1=0.381:2=0.017 ( ryan.cast  ryan.team  ryan.title )
 * #combine:0=0.927:1=0.070:2=0.002 ( war.cast  war.team  war.title ))
 *
 * @author jykim
 */
public class PRMSTraversal implements Traversal {

  private int levels;
  String[] fieldList;
  String[] weightList = null;
  Retrieval retrieval;

  public PRMSTraversal(Parameters parameters, Retrieval retrieval) {
    levels = 0;
    this.retrieval = retrieval;
  }

  public void beforeNode(Node original) throws Exception {
    levels++;
  }

  public Node afterNode(Node original) throws Exception {
    levels--;
    if (levels > 0) {
      return original;
    } else if (original.getOperator().equals("prms")) {

      // Fetch the field list parameter from the query
      fieldList = original.getParameters().get("fields").split(",");
      try
      {
        weightList = original.getParameters().get("weights").split(",");
      }
      catch(java.lang.IllegalArgumentException e)
      {
        
      }
      // Get the field length
      Map<String, Long> fieldLengths = new HashMap<String, Long>();
      for (String field : fieldList) {
        Parameters p = retrieval.getRetrievalStatistics();
        long f_length = Integer.parseInt(p.get("field."+field+"/collectionLength"));
        fieldLengths.put(field, f_length);
      }

      ArrayList<Node> children = original.getInternalNodes();
      ArrayList<Node> terms = new ArrayList<Node>();
      for (Node child : children) {
        ArrayList<Node> termFields = new ArrayList<Node>();
        Parameters weights = new Parameters();
        int i = 0;
        for (String field : fieldList) {

          Parameters par1 = new Parameters();
          par1.add("default", child.getDefaultParameter());
          par1.add("part", "field."+field);
          Node termCount = new Node("counts", par1, new ArrayList(), 0);
          if(weightList != null)
          {
            weights.set(Integer.toString(i), weightList[i]);
            //System.err.println("field weight : "+weightList[i]);
          }
          else
          {
            long f_term_field = retrieval.xCount(termCount);
            //System.err.println("f_term_field : "+f_term_field);
            double f_term_field_prob = (double) f_term_field / fieldLengths.get(field);
            weights.set(Integer.toString(i), Double.toString(f_term_field_prob));
          }
          termFields.add(termCount);
          i++;
        }
        Node termFieldNodes = new Node("combine", weights, termFields, 0);
        terms.add(termFieldNodes);
      }
      Node termNodes = new Node("combine", new Parameters(), terms, original.getPosition());

      return termNodes;
    } else {
      return original;
    }
  }
}
