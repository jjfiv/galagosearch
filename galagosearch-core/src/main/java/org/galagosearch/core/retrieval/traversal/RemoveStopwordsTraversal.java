// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.tupleflow.Parameters;

/**
 * Finds stopwords in a query and removes them.  This does not
 * attempt to remove stopwords from phrase operators.
 * 
 * @author trevor
 */
public class RemoveStopwordsTraversal implements Traversal {
    int level = 0;
    Stack<Integer> removableOperators = new Stack<Integer>();
    HashSet<String> words;

    public RemoveStopwordsTraversal(Parameters parameters, StructuredRetrieval retrieval) {
        List<String> wordsList = parameters.stringList("word");
        words = new HashSet<String>(wordsList);
    }

    public Node afterNode(Node node) throws Exception {
        ArrayList<Node> children = new ArrayList<Node>();

        if (node.getOperator().equals("combine")) {
            ArrayList<Node> oldChildren = node.getInternalNodes();
            for (int i = 0; i < oldChildren.size(); i++) {
                Node child = oldChildren.get(i);
                boolean isStopword = child.getOperator().equals("text") &&
                                     words.contains(child.getDefaultParameter());
                if (!isStopword) {
                    children.add(child);
                }
            }

            return new Node(node.getOperator(), children);
        }
        
        return node;
    }

    public void beforeNode(Node node) throws Exception {
        level++;
        if (node.getOperator().equals("combine")) {
            removableOperators.add(level);
        }
    }
}
