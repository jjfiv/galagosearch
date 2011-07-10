/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.traversal;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class RemoveStopwordsTraversalTest extends TestCase {

  public RemoveStopwordsTraversalTest(String testName) {
    super(testName);
  }

  public void testFileRemoval() throws Exception {
    File temp = Utility.createTemporary();

    PrintWriter writer = new PrintWriter(temp);
    writer.println("a");
    writer.println("b");
    writer.close();
    Parameters p = new Parameters();
    p.set("stopwords", temp.getCanonicalPath());
    RemoveStopwordsTraversal traversal = new RemoveStopwordsTraversal(p, null);
    Node root = StructuredQuery.parse("#combine(#counts:a() #counts:c() #counts:b() #counts:d() #counts:e())");
    Node removed = StructuredQuery.copy(traversal, root);

    assertEquals(5, removed.getInternalNodes().size());
    assertEquals(null, removed.getInternalNodes().get(0).getDefaultParameter());
    assertEquals("c", removed.getInternalNodes().get(1).getDefaultParameter());
    assertEquals(null, removed.getInternalNodes().get(2).getDefaultParameter());
    assertEquals("d", removed.getInternalNodes().get(3).getDefaultParameter());
    assertEquals("e", removed.getInternalNodes().get(4).getDefaultParameter());

    root = StructuredQuery.parse("#od:5(#extents:a() #extents:c() #extents: b())");
    removed = StructuredQuery.copy(traversal, root);
    assertEquals(1, removed.getInternalNodes().size());
    assertEquals("c", removed.getInternalNodes().get(0).getDefaultParameter());
    
    temp.delete();
  }

  public void testListRemoval() throws Exception {
    Parameters p = new Parameters();
    ArrayList<Value> values = new ArrayList<Value>();
    Value v;
    v = new Value();
    v.set("a");
    values.add(v);
    v = new Value();
    v.set("b");
    values.add(v);
    p.add("stopwords/word", values);

    RemoveStopwordsTraversal traversal = new RemoveStopwordsTraversal(p, null);
    Node root = StructuredQuery.parse("#combine(#counts:a() #counts:c() #counts:b() #counts:d() #counts:e())");
    Node removed = StructuredQuery.copy(traversal, root);

    assertEquals(5, removed.getInternalNodes().size());
    assertEquals(null, removed.getInternalNodes().get(0).getDefaultParameter());
    assertEquals("c", removed.getInternalNodes().get(1).getDefaultParameter());
    assertEquals(null, removed.getInternalNodes().get(2).getDefaultParameter());
    assertEquals("d", removed.getInternalNodes().get(3).getDefaultParameter());
    assertEquals("e", removed.getInternalNodes().get(4).getDefaultParameter());

    root = StructuredQuery.parse("#od:5(#extents:a() #extents:c() #extents: b())");
    removed = StructuredQuery.copy(traversal, root);
    assertEquals(1, removed.getInternalNodes().size());
    assertEquals("c", removed.getInternalNodes().get(0).getDefaultParameter());

  }
}
