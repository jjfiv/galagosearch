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


    public void testNestedCombineMerger() throws Exception {
        String query = "#combine(#combine:0=0.1:1=0.4(#text:a() #text:b()) #combine:0=150:1=350(#text:c() #text:d()))";
        Node result = StructuredQuery.parse(query);
        Node transformed = StructuredQuery.copy(new FlatteningTraversal(null, null), result);
        assertEquals("#combine:3=@/0.7/:2=@/0.3/:1=@/0.8/:0=@/0.2/( #text:a() #text:b() #text:c() #text:d() )", transformed.toString());
    }
}
