// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.tupleflow.Parameters;

/**
 * Looks at the current node and attempts to rewrite Indri-style
 * operators in the Galago format.  It can rewrite three types of expressions:
 * <ul>
 *  <li>#<i>n</i> changes to #od:</i>n</i></li>
 *  <li>#od<i>n</i> changes to #od:<i>n</i></li>
 *  <li>#uw<i>n</i> changes to #uw:<i>n</i></li>
 * </ul>
 * @author trevor
 */
public class IndriWindowCompatibilityTraversal implements Traversal {
    public IndriWindowCompatibilityTraversal(Parameters parameters, StructuredRetrieval retrieval) {
    }

    public Node afterNode(Node original) {
        String operator = original.getOperator();
        ArrayList<Node> children = original.getInternalNodes();

        if (operator.length() == 0) {
            return original;
        }

        if (Character.isDigit(operator.codePointAt(0))) {
            // this is a #n node, which is an ordered window node
            return new Node("od", operator, children, original.getPosition());
        } else if (operator.startsWith("od") &&
                operator.length() > 2 &&
                Character.isDigit(operator.codePointAt(2))) {
            // this is a #od3() node
            return new Node("od", operator.substring(2),
                    children, original.getPosition());
        } else if (operator.startsWith("uw") &&
                operator.length() > 2 &&
                Character.isDigit(operator.codePointAt(2))) {
            // this is a #uw3 node
            return new Node("uw", operator.substring(2),
                    children, original.getPosition());
        }

        return original;
    }

    public void beforeNode(Node object) throws Exception {
        // does nothing
    }
}
