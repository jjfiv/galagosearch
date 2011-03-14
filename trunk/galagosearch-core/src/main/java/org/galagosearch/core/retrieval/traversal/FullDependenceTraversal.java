// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.query.MalformedQueryException;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;

/**
 * Transforms a #fulldep operator into a full expansion of the
 * sequential dependence model. That means:
 * 
 * #fulldep( #text:term1() #text:term2() ... termk ) -->
 * 
 * #weight ( 0.8 #combine ( term1 term2 ... termk)
 *           0.15 #combine ( #od(term1 term2) #od(term2 term3) #od(term1 term3) ... #od(term1 ... termk) )
 *           0.05 #combine ( #uw8(term1 term2) #uw8(term1 term2) #uw8(term1 term3) ... #uw8(term1 ... termk) ) )
 *
 *
 *
 * @author sjh
 */
@RequiredStatistics(statistics = {"windowLimit", "uniw", "odw", "uww"})
public class FullDependenceTraversal implements Traversal {

  private int defaultWindowLimit;

  private int levels;
  private String unigramDefault;
  private String orderedDefault;
  private String unorderedDefault;

  public FullDependenceTraversal(Parameters parameters, Retrieval retrieval) {
    levels = 0;
    unigramDefault = parameters.get("uniw", "0.8");
    orderedDefault = parameters.get("odw", "0.15");
    unorderedDefault = parameters.get("uww", "0.05");

    defaultWindowLimit = (int) parameters.get("windowLimit", -1);
  }

  public void beforeNode(Node original) throws Exception {
    levels++;
  }

  public Node afterNode(Node original) throws Exception {
    levels--;
    if (levels > 0) {
      return original;
    } else if (original.getOperator().equals("fulldep")) {
    
      int windowLimit = (int) original.getParameters().get( "windowLimit", defaultWindowLimit );

      // First check format - should only contain text node children
      ArrayList<Node> children = original.getInternalNodes();
      for (Node child : children) {
        if (child.getOperator().equals("text") == false) {
          throw new MalformedQueryException("seqdep operator needs text-only children");
        }
      }

      // formatting is ok - now reassemble
      // unigrams go as-is
      Node unigramNode = new Node("combine", children);

      // if we only have one child, return the unigram node.
      if (children.size() == 1) {
        return unigramNode;
      }

      // ordered and unordered can go at the same time
      ArrayList<Node> ordered = new ArrayList<Node>();
      ArrayList<Node> unordered = new ArrayList<Node>();

      ArrayList<ArrayList<Node>> nodePowerSet = powerSet(new ArrayList(children));
      for (ArrayList<Node> set : nodePowerSet) {
        if((windowLimit < 2) || (windowLimit >= set.size())) {
          if(set.size() >= 2) {
            int uwSize = 4 * set.size();
            ordered.add(new Node("ordered", "1", set));
            unordered.add(new Node("unordered", Integer.toString(uwSize), set));
          }
        }
      }

      Node orderedWindowNode = new Node("combine", ordered);
      Node unorderedWindowNode = new Node("combine", unordered);

      // now get the weights for each component, and add to immediate children
      Parameters parameters = original.getParameters();
      String uni = parameters.get("uniw", unigramDefault);
      String odw = parameters.get("odw", orderedDefault);
      String uww = parameters.get("uww", unorderedDefault);

      Parameters weights = new Parameters();
      ArrayList<Node> immediateChildren = new ArrayList<Node>();

      // unigrams - 0.80
      weights.set("0", uni);
      immediateChildren.add(unigramNode);

      // ordered
      weights.set("1", odw);
      immediateChildren.add(orderedWindowNode);

      // unordered
      weights.set("2", uww);
      immediateChildren.add(unorderedWindowNode);

      // Finally put them all inside a comine node w/ the weights
      Node outerweight = new Node("combine", weights, immediateChildren, original.getPosition());
      return outerweight;
    } else {
      return original;
    }
  }

  private ArrayList<ArrayList<Node>> powerSet(ArrayList<Node> children) {
    // base case
    ArrayList<ArrayList<Node>> ps = new ArrayList();

    if (children.isEmpty()) {
      ps.add(new ArrayList());
    } else {
      Node n = children.remove(0);
      ArrayList<ArrayList<Node>> subps = powerSet(children);
      for (ArrayList<Node> set : subps) {
        // add a clone of the original
        ps.add(new ArrayList(set));
        // add the original + node n
        set.add(0, n);
        ps.add(set);
      }
    }
    return ps;
  }
}
