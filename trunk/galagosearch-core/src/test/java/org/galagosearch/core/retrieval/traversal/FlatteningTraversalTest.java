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
public class FlatteningTraversalTest extends TestCase {
    
    public FlatteningTraversalTest(String testName) {
        super(testName);
    }

    public void testNestedWindowRewrite() throws Exception {
        String query = "#uw:5( #od:1(#text:a() #text:b()) )";
        Node result = StructuredQuery.parse(query);
        Node transformed = StructuredQuery.copy(new FlatteningTraversal(null, null), result);
        assertEquals("#od:1( #text:a() #text:b() )", transformed.toString());
    }
}
