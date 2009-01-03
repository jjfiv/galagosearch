/*
 * ComplexQueryTest.java
 * JUnit based test
 *
 * Created on August 10, 2007, 8:40 PM
 */

package org.galagosearch.core.retrieval.query;

import org.galagosearch.core.retrieval.query.StructuredQuery;
import org.galagosearch.core.retrieval.query.Node;
import junit.framework.*;
import java.util.ArrayList;
import org.galagosearch.core.retrieval.query.Traversal;

/**
 *
 * @author trevor
 */
public class StructuredQueryTest extends TestCase {
    public StructuredQueryTest(String testName) {
        super(testName);
    }
    
    public static class SimpleCopyTraversal implements Traversal {
        public void beforeNode( Node node ) {
            // do nothing
        }
        
        public Node afterNode( Node node, ArrayList<Node> children ) {
            return new Node( node.getOperator(), node.getParameters(), children, node.getPosition() );
        }
    }
    
    public static class NullTraversal implements Traversal {
        public void beforeNode( Node n ) {}
        public Node afterNode( Node n, ArrayList<Node> an ) { return null; }
    }
    
    public static Node createQuery() {
        Node childB = new Node( "text", "b", 0 );
        Node childA = new Node( "text", "a", 0 );
        ArrayList<Node> childList = new ArrayList();
        childList.add(childA);
        Node featureA = new Node( "feature", "bm25", childList, 0 );
        ArrayList<Node> featureList = new ArrayList<Node>();
        featureList.add(featureA);
        featureList.add(childB);
        Node tree = new Node( "combine", featureList, 0 );
    
        return tree;
    }
    
    public void testCopy() throws Exception {
        Traversal traversal = new SimpleCopyTraversal();
        Node tree = createQuery();
        Node result = StructuredQuery.copy(traversal, tree);
        
        assertEquals(tree, result);
    }

    public void testWalk() throws Exception {
        Traversal traversal = new NullTraversal();
        Node tree = createQuery();
        
        StructuredQuery.walk(traversal, tree);
    }

    public void testSimpleParse() {
        String query = "#combine( #feature:bm25(a) b )";
        Node tree = createQuery();
        
        Node result = StructuredQuery.parse(query);
        assertEquals(tree, result);
    }
    
    public void testFieldParse() {
        String query = "#combine( a.b c.d @/e/ @/f. h/.g )";
        Node result = StructuredQuery.parse(query);
        assertEquals(
            "#combine( #inside( #text:a() #field:b() ) #inside( #text:c() #field:d() ) #text:e() #inside( #text:@/f. h/() #field:g() ) )",
            result.toString());
    }
    
    public void testFieldCombinationParse() {
        String query = "a.b.c";
        Node result = StructuredQuery.parse(query);
        assertEquals(
                "#inside( #inside( #text:a() #field:b() ) #field:c() )",
                result.toString());
    }
    
    public void testFieldCombinationParseCommas() {
        String query = "a.b,c";
        Node result = StructuredQuery.parse(query);
        assertEquals(
                "#inside( #text:a() #extentor( #field:b() #field:c() ) )",
                result.toString());
    }
    
    public void testParensParse() {
        String query = "a.(b) a.(b,c)";
        Node result = StructuredQuery.parse(query);
        assertEquals(
                "#combine( #smoothlm( #text:a() #field:b() ) #smoothlm( #text:a() #extentor( #field:b() #field:c() ) ) )",
                result.toString());
    }
}
