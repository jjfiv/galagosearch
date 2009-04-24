// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;

/**
 *
 * @author trevor
 */
public class WeightConversionTraversalTest extends TestCase {
    
    public WeightConversionTraversalTest(String testName) {
        super(testName);
    }

    /**
     * Test of afterNode method, of class WeightConversionTraversal.
     */
    public void testAfterNonWeighted() throws Exception {
        ArrayList<Node> internalNodes = new ArrayList<Node>();
        internalNodes.add(new Node("littlenode", "null"));
        Node root = new Node("bignode", internalNodes);
        
        WeightConversionTraversal traversal = new WeightConversionTraversal(null, null);
        Node result = traversal.afterNode(root);
        assertEquals(root, result);
    }

    public void testAfterWeighted() throws Exception {
        ArrayList<Node> internalNodes = new ArrayList<Node>();
        internalNodes.add(new Node("text", "1.0"));
        internalNodes.add(new Node("text", "dog"));
        Node root = new Node("weight", internalNodes);
        
        ArrayList<Node> expectedVeryInternal = new ArrayList<Node>();
        expectedVeryInternal.add(new Node("text", "dog"));
        ArrayList<Node> expectedInternal = new ArrayList<Node>();
        expectedInternal.add(new Node("scale", "1.0", expectedVeryInternal));
        Node expected = new Node("combine", expectedInternal);
        
        WeightConversionTraversal traversal = new WeightConversionTraversal(null, null);
        Node result = traversal.afterNode(root);
        assertEquals(expected, result);
    }

    public void testRealDecimals() throws Exception {
        Node root = StructuredQuery.parse("#weight(1.5 dog 2.0 cat)");
        assertEquals("#weight( #inside( #text:1() #field:5() ) #text:dog() #inside( #text:2() #field:0() ) #text:cat() )", root.toString());

        WeightConversionTraversal traversal = new WeightConversionTraversal(null, null);
        Node result = StructuredQuery.copy(traversal, root);
        assertEquals("#combine( #scale:@/1.5/( #text:dog() ) #scale:@/2.0/( #text:cat() ) )", result.toString());
    }

    public void testRealIntegers() throws Exception {
        Node root = StructuredQuery.parse("#weight(1 dog 2 cat)");
        assertEquals("#weight( #text:1() #text:dog() #text:2() #text:cat() )", root.toString());

        WeightConversionTraversal traversal = new WeightConversionTraversal(null, null);
        Node result = StructuredQuery.copy(traversal, root);
        assertEquals("#combine( #scale:1( #text:dog() ) #scale:2( #text:cat() ) )", result.toString());
    }
}
