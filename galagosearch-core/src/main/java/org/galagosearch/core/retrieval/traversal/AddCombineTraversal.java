// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.tupleflow.Parameters;

/**
 * Adds a #combine at the top level of the query.
 * @author trevor
 */
public class AddCombineTraversal implements Traversal {
    int levels = 0;

    public AddCombineTraversal(Parameters parameters, StructuredRetrieval retrieval) {
    }

    public void beforeNode(Node object) throws Exception {
        levels++;
    }

    public Node afterNode(Node original) throws Exception {
        levels--;
        if (levels > 0) {
            return original;
        } else if (!original.getOperator().equals("combine")) {
            // Only add a combine if the top level operator is not already a combine
            ArrayList<Node> originalChild = new ArrayList<Node>();
            originalChild.add(original);
            return new Node("combine", originalChild);
        } else {
            return original;
        }
    }
}
