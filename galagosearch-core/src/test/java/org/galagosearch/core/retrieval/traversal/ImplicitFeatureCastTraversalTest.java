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
    public void tearDown() {
        indexPath.delete();
    }

    public void testTraversal() throws Exception {
        StructuredIndex index = new StructuredIndex(indexPath.getAbsolutePath());
        StructuredRetrieval retrieval = new StructuredRetrieval(index);
        ImplicitFeatureCastTraversal traversal = new ImplicitFeatureCastTraversal(retrieval);
        Node tree = StructuredQuery.parse("#combine(cat dog.title)");
        // TODO: fix this test
        //Node result = StructuredQuery.copy(traversal, tree);
    }
}
