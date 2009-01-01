/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.traversal;

import org.galagosearch.core.retrieval.traversal.WeightConversionTraversal;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.query.Node;

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
        
        WeightConversionTraversal traversal = new WeightConversionTraversal();
        Node result = traversal.afterNode(root, internalNodes);
        assertEquals(root, result);
    }

    public void testAfterWeighted() throws Exception {
        ArrayList<Node> internalNodes = new ArrayList<Node>();
        internalNodes.add(new Node("text", "1.0"));
        internalNodes.add(new Node("text", "dog"));
        Node root = new Node("wsyn", internalNodes);
        
        ArrayList<Node> expectedVeryInternal = new ArrayList<Node>();
        expectedVeryInternal.add(new Node("text", "dog"));
        ArrayList<Node> expectedInternal = new ArrayList<Node>();
        expectedInternal.add(new Node("scale", "1.0", expectedVeryInternal));
        Node expected = new Node("wsyn", expectedInternal);
        
        WeightConversionTraversal traversal = new WeightConversionTraversal();
        Node result = traversal.afterNode(root, internalNodes);
        assertEquals(expected, result);
    }
}
