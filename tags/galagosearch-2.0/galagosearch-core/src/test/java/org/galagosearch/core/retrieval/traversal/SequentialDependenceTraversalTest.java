// BSD License (http://www.galagosearch.org/license)

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
 * @author trevor
 */
public class SequentialDependenceTraversalTest extends TestCase {
    File indexPath;

    public SequentialDependenceTraversalTest(String testName) {
        super(testName);
    }

    @Override
    public void setUp() throws FileNotFoundException, IOException {
        indexPath = StructuredRetrievalTest.makeIndex();
    }

    @Override
    public void tearDown() throws IOException{
        Utility.deleteDirectory(indexPath);
    }

    public void testTraversal() throws Exception {
        StructuredIndex index = new StructuredIndex(indexPath.getAbsolutePath());
        StructuredRetrieval retrieval = new StructuredRetrieval(index, new Parameters());

        // first try a single word - no windows possible
        Parameters p = new Parameters();
        p.add("retrievalGroup","test");
        SequentialDependenceTraversal traversal = new SequentialDependenceTraversal(p, retrieval);
        Node tree = StructuredQuery.parse("#seqdep( cat )");
        StringBuilder transformed = new StringBuilder();
        transformed.append("#combine( #text:cat() )");
        Node result = StructuredQuery.copy(traversal, tree);

        assertEquals(transformed.toString(), result.toString());

        // now try a vanilla query
        tree = StructuredQuery.parse("#seqdep( cat dog rat )");
        transformed = new StringBuilder();
        transformed.append("#combine:2=@/0.05/:1=@/0.15/:0=@/0.8/( ");
        transformed.append("#combine( #text:cat() #text:dog() #text:rat() ) ");
        transformed.append("#combine( #ordered:1( #text:cat() #text:dog() ) #ordered:1( #text:dog() #text:rat() ) ) ");
        transformed.append("#combine( #unordered:8( #text:cat() #text:dog() ) #unordered:8( #text:dog() #text:rat() ) ) )");
        result = StructuredQuery.copy(traversal, tree);

        assertEquals(transformed.toString(), result.toString());

        // now change weights
        p.set("uniw", "0.75");
        p.set("odw", "0.10");
        p.set("uww", "0.15");
        traversal = new SequentialDependenceTraversal(p, retrieval);
        tree = StructuredQuery.parse("#seqdep( cat dog rat )");
        transformed = new StringBuilder();
        transformed.append("#combine:2=@/0.15/:1=@/0.10/:0=@/0.75/( ");
        transformed.append("#combine( #text:cat() #text:dog() #text:rat() ) ");
        transformed.append("#combine( #ordered:1( #text:cat() #text:dog() ) #ordered:1( #text:dog() #text:rat() ) ) ");
        transformed.append("#combine( #unordered:8( #text:cat() #text:dog() ) #unordered:8( #text:dog() #text:rat() ) ) )");
        result = StructuredQuery.copy(traversal, tree);

        assertEquals(transformed.toString(), result.toString());

        // now change weights via the operator
        tree = StructuredQuery.parse("#seqdep:uniw=0.55:odw=0.27:uww=0.18( cat dog rat )");
        transformed = new StringBuilder();
        transformed.append("#combine:2=@/0.18/:1=@/0.27/:0=@/0.55/( ");
        transformed.append("#combine( #text:cat() #text:dog() #text:rat() ) ");
        transformed.append("#combine( #ordered:1( #text:cat() #text:dog() ) #ordered:1( #text:dog() #text:rat() ) ) ");
        transformed.append("#combine( #unordered:8( #text:cat() #text:dog() ) #unordered:8( #text:dog() #text:rat() ) ) )");
        result = StructuredQuery.copy(traversal, tree);

        assertEquals(transformed.toString(), result.toString());

    }
}
