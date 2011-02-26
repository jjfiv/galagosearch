// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.StatisticsGatherer;
import java.io.IOException;
import junit.framework.*;
import org.galagosearch.core.retrieval.structured.CountValueIterator;

/**
 *
 * @author trevor
 */
public class StatisticsGathererTest extends TestCase {
    public StatisticsGathererTest(String testName) {
        super(testName);
    }
    
    public void testGather() throws IOException {
        int[][] data = { { 1, 5 }, { 3, 7 } };
        CountValueIterator iterator = new FakeExtentIterator( data );
        StatisticsGatherer instance = new StatisticsGatherer( iterator );
        instance.run();
        
        assertEquals( instance.getTermCount(), 2 );
        assertEquals( instance.getDocumentCount(), 2 );
    }
}
