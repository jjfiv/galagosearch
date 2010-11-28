// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.query.MalformedQueryException;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.tupleflow.Parameters;

/**
 * Transforms a #seqdep operator into a full expansion of the
 * sequential dependence model. That means:
 * 
 * #seqdep( #text:term1() #text:term2() ... termk ) -->
 * 
 * #weight ( 0.8 #combine ( term1 term2 ... termk)
 *           0.15 #combine ( #od(term1 term2) #od(term2 term3) ... #od(termk-1 termk) )
 *           0.05 #combine ( #uw8(term term2) ... #uw8(termk-1 termk) ) )
 *
 *
 *
 * @author irmarc
 */
public class SequentialDependenceTraversal implements Traversal {
    private int levels;


    public SequentialDependenceTraversal(Parameters parameters, StructuredRetrieval retrieval) {
        levels = 0;
    }

    public void beforeNode(Node original) throws Exception {
        levels++;
    }

    public Node afterNode(Node original) throws Exception {
        levels--;
        if (levels > 0) {
            return original;
        } else if (original.getOperator().equals("seqdep")) {
                // get to work

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

                // ordered and unordered can go at the same time
                ArrayList<Node> ordered = new ArrayList<Node>();
                ArrayList<Node> unordered= new ArrayList<Node>();

                for (int i = 0; i < (children.size()-1); i++) {
                    ArrayList<Node> pair = new ArrayList<Node>();
                    pair.add(children.get(i));
                    pair.add(children.get(i+1));
                    ordered.add(new Node("ordered", "1", pair));
                    unordered.add(new Node("unordered", "8", pair));
                }

                Node orderedWindowNode = new Node("combine", ordered);
                Node unorderedWindowNode = new Node("combine", unordered);

                // now get the weights for each component, and add to immediate children
                Parameters parameters = original.getParameters();
                String uni = parameters.get("uniw", "0.8");
                String odw = parameters.get("odw", "0.15");
                String uww = parameters.get("uww", "0.05");

                ArrayList<Node> immediateChildren = new ArrayList<Node>();
                
                // unigrams - 0.80
                immediateChildren.add(new Node("text", uni));
                immediateChildren.add(unigramNode);

                // ordered
                immediateChildren.add(new Node("text", odw));
                immediateChildren.add(orderedWindowNode);

                // unordered
                immediateChildren.add(new Node("text", uww));
                immediateChildren.add(unorderedWindowNode);

                // Finally put them all inside a weight node
                Node outerweight = new Node("weight", immediateChildren);
                return outerweight;
        } else {
            return original;
        }
    }
}
