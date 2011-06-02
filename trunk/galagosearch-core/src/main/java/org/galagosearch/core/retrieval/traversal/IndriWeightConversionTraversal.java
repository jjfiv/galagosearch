// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class IndriWeightConversionTraversal implements Traversal {
    public IndriWeightConversionTraversal(Parameters parameters, Retrieval retrieval) {
    }

    public void beforeNode(Node object) throws Exception {
        // do nothing
    }

    public String getWeight(Node weightNode) {
        if (weightNode.getOperator().equals("inside")) {
            if (weightNode.getInternalNodes().size() != 2) {
                return "1";
            } else {
                Node inner = weightNode.getInternalNodes().get(0);
                Node outer = weightNode.getInternalNodes().get(1);
                return inner.getDefaultParameter() + "." + outer.getDefaultParameter();
            }
        } else {
            return weightNode.getDefaultParameter();
        }
    }

    public Node afterNode(Node node) throws Exception {
        if (node.getOperator().equals("weight")) {
            ArrayList<Node> children = node.getInternalNodes();

            // first, verify that the appropriate children are weights
            if (children.size() % 2 == 1) {
                throw new IOException("A weighted node cannot have an odd number of internal nodes: " +
                        node.getInternalNodes().size());
            }
                        
            // now, reassemble everything:
            Parameters newParameters = node.getParameters();
            ArrayList<Node> newChildren = new ArrayList<Node>();
            for (int i = 0; i < children.size(); i += 2) {
                Node weightNode = children.get(i);
                Node childNode = children.get(i+1);
                
                newChildren.add(childNode);
                newParameters.set(Integer.toString(i/2), getWeight(weightNode));
            }

            return new Node("combine", newParameters, newChildren, node.getPosition());
        } else {
            return node;
        }
    }
}
