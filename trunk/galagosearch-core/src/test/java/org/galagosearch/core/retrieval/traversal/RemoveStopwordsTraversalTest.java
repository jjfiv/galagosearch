/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Parameters.Value;

/**
 *
 * @author trevor
 */
public class RemoveStopwordsTraversalTest extends TestCase {
    
    public RemoveStopwordsTraversalTest(String testName) {
        super(testName);
    }

    public void testRemoval() throws Exception {
        Parameters p = new Parameters();
        ArrayList<Value> values = new ArrayList<Value>();
        Value v;
        v = new Value();
        v.set("a");
        values.add(v);
        v = new Value();
        v.set("b");
        values.add(v);
        p.add("word", values);

        RemoveStopwordsTraversal traversal = new RemoveStopwordsTraversal(p, null);
        Node root = StructuredQuery.parse("#combine(a c b d e)");
        Node removed = StructuredQuery.copy(traversal, root);

        assertEquals("c", removed.getInternalNodes().get(0).getDefaultParameter());
        assertEquals("d", removed.getInternalNodes().get(1).getDefaultParameter());
        assertEquals("e", removed.getInternalNodes().get(2).getDefaultParameter());

        root = StructuredQuery.parse("#ow:5(a c b)");
        removed = StructuredQuery.copy(traversal, root);
        assertEquals("a", removed.getInternalNodes().get(0).getDefaultParameter());
        assertEquals("c", removed.getInternalNodes().get(1).getDefaultParameter());
        assertEquals("b", removed.getInternalNodes().get(2).getDefaultParameter());
    }
}
