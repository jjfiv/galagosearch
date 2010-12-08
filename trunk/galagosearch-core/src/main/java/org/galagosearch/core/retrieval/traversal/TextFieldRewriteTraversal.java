// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.traversal;

import java.io.IOException;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.Traversal;
import org.galagosearch.core.retrieval.Retrieval;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.util.TextPartAssigner;
import org.galagosearch.tupleflow.Parameters;

/**
 * <p>StructuredQuery.parse parses queries using pseudo-operators, like #text and #field, so
 * that <tt>a.b.</tt> becomes <tt>#inside( #text:a() #field:b() )</p>.  These pseudo-operators
 * are not supported by common index types.  This traversal renames <tt>#text</tt> and
 * <tt>#field</tt> to something sensible.</p>
 * 
 * @author trevor
 */
@RequiredStatistics(statistics = {"retrievalGroup"})
public class TextFieldRewriteTraversal implements Traversal {

  Parameters availableParts;

  public TextFieldRewriteTraversal(Parameters parameters, Retrieval retrieval) throws IOException {
    this.availableParts = retrieval.getAvailiableParts(parameters.get("retrievalGroup"));

  }

  public void beforeNode(Node object) throws Exception {
    // do nothing
  }

  public Node afterNode(Node original) throws Exception {
    String operator = original.getOperator();

    if (operator.equals("text")) {
      return TextPartAssigner.assignPart(original, availableParts);
    } else if (operator.equals("field") || operator.equals("any")) {
      if (availableParts.stringList("part").contains("extents")) { 
        return TextPartAssigner.transformedNode(original, "extents", "extents");
      } else {
        return original;
      }
    } else {
      return original;
    }
  }
}
