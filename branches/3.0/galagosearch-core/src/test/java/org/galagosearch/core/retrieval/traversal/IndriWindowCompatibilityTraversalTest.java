/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.traversal;

import junit.framework.TestCase;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.StructuredQuery;

/**
 *
 * @author trevor
 */
public class IndriWindowCompatibilityTraversalTest extends TestCase {
    
    public IndriWindowCompatibilityTraversalTest(String testName) {
        super(testName);
    }

    public void testIndriPoundNRewrite() throws Exception {
        String query = "#3()";
        Node result = StructuredQuery.parse(query);
        Node transformed = StructuredQuery.copy(new IndriWindowCompatibilityTraversal(null, null), result);
        assertEquals("#od:3()", transformed.toString());
    }

    public void testIndriOdNRewrite() throws Exception {
        String query = "#od3()";
        Node result = StructuredQuery.parse(query);
        Node transformed = StructuredQuery.copy(new IndriWindowCompatibilityTraversal(null, null), result);
        assertEquals("#od:3()", transformed.toString());
    }

    public void testIndriUwNRewrite() throws Exception {
        String query = "#uw5()";
        Node result = StructuredQuery.parse(query);
        Node transformed = StructuredQuery.copy(new IndriWindowCompatibilityTraversal(null, null), result);
        assertEquals("#uw:5()", transformed.toString());
    }
}
