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
public class ImplicitFeatureCastTraversalTest extends TestCase {
    File indexPath;

    public ImplicitFeatureCastTraversalTest(String testName) {
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

    // Also tests the TextFieldRewriteTraversal
    public void testTraversal() throws Exception {
        StructuredIndex index = new StructuredIndex(indexPath.getAbsolutePath());
        StructuredRetrieval retrieval = new StructuredRetrieval(index, new Parameters());

        Parameters p = new Parameters();
        p.add("retrievalGroup","test");
        ImplicitFeatureCastTraversal traversal = new ImplicitFeatureCastTraversal(p, retrieval);
        TextFieldRewriteTraversal precedes = new TextFieldRewriteTraversal(p, retrieval);
        Node tree = StructuredQuery.parse("#combine( cat dog.title)");
        tree = StructuredQuery.copy(precedes, tree); // converts #text to #extents...
        StringBuilder transformed = new StringBuilder();
        transformed.append("#combine( ");
        transformed.append("#feature:dirichlet( #counts:cat:part=postings() ) ");
        transformed.append("#feature:dirichlet( #inside( #extents:dog:part=postings() ");
        transformed.append("#extents:title:part=extents() ) ) )");
        Node result = StructuredQuery.copy(traversal, tree);
        assertEquals(transformed.toString(), result.toString());
    }
}
