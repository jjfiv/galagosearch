// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.query.MalformedQueryException;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.tupleflow.Parameters;

/**
 * Transforms an #inside operator into a
 *  #extents operator using the field parts.
 *
 *
 * @author sjh
 */
public class InsideToFieldPartTraversal implements Traversal {

  HashSet<String> parts = new HashSet();

  public InsideToFieldPartTraversal(Parameters parameters, Retrieval retrieval) throws IOException {
    Parameters availableParts = retrieval.getAvailableParts("all");
    parts = new HashSet(availableParts.stringList("part"));
  }

  public void beforeNode(Node original) throws Exception {
  }

  public Node afterNode(Node original) throws Exception {
    if (original.getOperator().equals("inside")) {
      ArrayList<Node> children = original.getInternalNodes();

      if (children.size() != 2) {
        return original;
      }

      Node text = children.get(0);
      Node field = children.get(1);

      if (!parts.contains("field." + field.getDefaultParameter())) {
        return original;
      }

      Parameters p = new Parameters();
      p.add("default", text.getDefaultParameter());
      p.add("part", "field." + field.getDefaultParameter());

      Node newNode = new Node("extents", p, new ArrayList(), original.getPosition());

      return newNode;
    } else {
      return original;
    }
  }
}
