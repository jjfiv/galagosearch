// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.traversal;

import java.util.ArrayList;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.tupleflow.Parameters;

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
        
        IndriWeightConversionTraversal traversal = new IndriWeightConversionTraversal(null, null);
        Node result = traversal.afterNode(root);
        assertEquals(root, result);
    }

    public void testAfterWeighted() throws Exception {
        ArrayList<Node> internalNodes = new ArrayList<Node>();
        internalNodes.add(new Node("text", "1.0"));
        internalNodes.add(new Node("text", "dog"));
        Node root = new Node("weight", internalNodes);

        ArrayList<Node> expectedInternal = new ArrayList();
        expectedInternal.add( new Node("text", "dog") );
        Parameters expectedParameters = new Parameters();
        expectedParameters.add("0", "1.0");
        Node expected = new Node("combine", expectedParameters, expectedInternal, 0);
        
        IndriWeightConversionTraversal traversal = new IndriWeightConversionTraversal(null, null);
        Node result = traversal.afterNode(root);
        assertEquals(expected, result);
    }

    public void testRealDecimals() throws Exception {
        Node root = StructuredQuery.parse("#weight(1.5 dog 2.0 cat)");
        assertEquals("#weight( #inside( #text:1() #field:5() ) #text:dog() #inside( #text:2() #field:0() ) #text:cat() )", root.toString());

        IndriWeightConversionTraversal traversal = new IndriWeightConversionTraversal(null, null);
        Node result = StructuredQuery.copy(traversal, root);
        assertEquals("#combine:1=@/2.0/:0=@/1.5/( #text:dog() #text:cat() )", result.toString());
    }

    public void testRealIntegers() throws Exception {
        Node root = StructuredQuery.parse("#weight(1 dog 2 cat)");
        assertEquals("#weight( #text:1() #text:dog() #text:2() #text:cat() )", root.toString());

        IndriWeightConversionTraversal traversal = new IndriWeightConversionTraversal(null, null);
        Node result = StructuredQuery.copy(traversal, root);
        assertEquals("#combine:1=2:0=1( #text:dog() #text:cat() )", result.toString());
    }
}
