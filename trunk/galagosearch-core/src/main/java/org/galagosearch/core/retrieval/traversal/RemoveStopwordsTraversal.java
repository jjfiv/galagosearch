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

  Stack<Integer> removableOperators = new Stack<Integer>();
  HashSet<String> words;
  HashSet<String> conjops;

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

    conjops = new HashSet();
    conjops.add("inside");
    conjops.add("ordered");
    conjops.add("od");
    conjops.add("unordered");
    conjops.add("uw");
    conjops.add("all");

    for(String w : words){
      System.err.println( w );
    }
  }

  public Node afterNode(Node node) throws Exception {
 
    // if the node is a stopword - replace with 'null' operator
    if ((node.getOperator().equals("counts")
            || node.getOperator().equals("extents"))
            && words.contains(node.getDefaultParameter())) {
      return new Node("null", new ArrayList());
    }

    // now if we have a conjunction node, we need to remove any null op children.
    ArrayList<Node> children = node.getInternalNodes();
    ArrayList<Node> newChildren = new ArrayList();
    for (Node child : children) {
      if (!child.getOperator().equals("null")) {
        newChildren.add(child);
      }
    }

    boolean hasNull = children.size() > newChildren.size();

    if (hasNull && conjops.contains(node.getOperator())) {
      // special case: inside 
      if (node.getOperator().equals("inside")) {
        return new Node("null", new ArrayList());
      }

      // all other cases - create a new list of non-null children
      if (newChildren.size() == 0) {
        return new Node("null", new ArrayList());
      } else {
        return new Node(node.getOperator(), node.getParameters(), newChildren, node.getPosition());
      }
    }

    // otherwise return the original
    return node;
  }

  public void beforeNode(Node node) throws Exception {
    // nothing
  }
}
