// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.traversal;

import java.io.File;
import java.io.IOException;
import java.lang.String;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * Finds stopwords in a query and removes them.  This does not
 * attempt to remove stopwords from phrase operators.
 * 
 * @author trevor
 */
@RequiredStatistics(statistics = {"stopwords"})
public class RemoveStopwordsTraversal implements Traversal {
    int level = 0;
    Stack<Integer> removableOperators = new Stack<Integer>();
    HashSet<String> words;
    HashSet<String> combiners;

    public RemoveStopwordsTraversal(Parameters parameters, Retrieval retrieval) {
        // Look for a file first
        String value = parameters.get("stopwords", "null");
        File f = new File(value);
        if (f.exists()) {
          try {
            words = Utility.readFileToStringSet(f);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        } else {
          List<String> wordsList = parameters.stringList("stopwords/word");
          words = new HashSet<String>(wordsList);
        }
        combiners = new HashSet<String>();
        combiners.add("combine");
        combiners.add("root");
    }

    public Node afterNode(Node node) throws Exception {
        ArrayList<Node> children = new ArrayList<Node>();

        if (combiners.contains(node.getOperator())) {
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
