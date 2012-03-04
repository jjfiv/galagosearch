// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.CountIterator;
import org.galagosearch.core.retrieval.structured.ScoreIterator;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.tupleflow.Parameters;

/**
 * For many kinds of queries, it may be preferable to not have to type
 * an explicit #feature operator around a count or extents term.  For example,
 * we want #combine(#feature:dirichlet(#counts:dog())) to be the same as
 * #combine(dog).  This transformation automatically adds the #feature:dirichlet
 * operator.
 * 
 * @author trevor
 */
public class ImplicitFeatureCastTraversal implements Traversal {
    StructuredRetrieval retrieval;

    public ImplicitFeatureCastTraversal(Parameters parameters, StructuredRetrieval retrieval) {
        this.retrieval = retrieval;
    }
    
    Node createSmoothingNode(Node child) {
        ArrayList<Node> data = new ArrayList<Node>();
        data.add(child);
        return new Node("feature", "dirichlet", data, child.getPosition());
    }
    
    public boolean isCountNode(Node node) throws Exception {
        NodeType nodeType = retrieval.getNodeType(node);
        if (nodeType == null) return false;
        Class outputClass = nodeType.getIteratorClass();
        return CountIterator.class.isAssignableFrom(outputClass);
    }

    public void beforeNode(Node node) throws Exception {
    }

    public Node afterNode(Node node) throws Exception {
        ArrayList<Node> newChildren = new ArrayList<Node>();
        NodeType nodeType = retrieval.getNodeType(node);
        if (nodeType == null) return node;
        ArrayList<Node> children = node.getInternalNodes();
        // Given that we're going to pass children.size() + 1 parameters to
        // this constructor, what types should those parameters have?
        Class[] types = nodeType.getParameterTypes(children.size() + 1);
        if (types == null) return node;

        for (int i = 1; i < types.length; ++i) {
            Node child = children.get(i-1);
            // If the parent will expect a ScoreIterator at this position, but
            // we've got a CountIterator here, we'll perform a conversion step.
            if (ScoreIterator.class.isAssignableFrom(types[i]) &&
                isCountNode(children.get(i-1))) {
                Node feature = createSmoothingNode(child);
                newChildren.add(feature);
            } else {
                newChildren.add(child);
            }
        }

        return new Node(node.getOperator(), node.getParameters(),
                        newChildren, node.getPosition());
    }
}
