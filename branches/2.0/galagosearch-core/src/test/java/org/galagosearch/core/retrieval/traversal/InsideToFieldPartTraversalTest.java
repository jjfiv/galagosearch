/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.retrieval.traversal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.StructuredRetrievalTest;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.structured.StructuredRetrieval;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class InsideToFieldPartTraversalTest extends TestCase {

  private File indexPath;

  public InsideToFieldPartTraversalTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws FileNotFoundException, IOException {
    indexPath = StructuredRetrievalTest.makeIndex();
  }

  @Override
  public void tearDown() throws IOException {
    Utility.deleteDirectory(indexPath);
  }

  public void testTraversal() throws Exception {
    StructuredIndex index = new StructuredIndex(indexPath.getAbsolutePath());
    StructuredRetrieval retrieval = new StructuredRetrieval(index, new Parameters());

    Parameters p = new Parameters();
    p.add("retrievalGroup", "test");

    InsideToFieldPartTraversal traversal = new InsideToFieldPartTraversal(p, retrieval);
    traversal.parts.add("field.subject");

    Node q1 = StructuredQuery.parse("#combine( cat dog.title donkey.subject)");
    Node q2 = StructuredQuery.copy(traversal, q1); // converts #text to #extents...

    StringBuilder transformed = new StringBuilder();

    transformed.append("#combine( ");
    transformed.append("#text:cat() ");
    transformed.append("#inside( #text:dog() ");
    transformed.append("#field:title() ) ");
    transformed.append("#extents:donkey:part=@/field.subject/() )");

    assertEquals(transformed.toString(), q2.toString());
  }
}
