// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.extents;

import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.structured.ScoreCombinationIterator;
import org.galagosearch.core.retrieval.structured.FilteredCombinationIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class FilteredCombinationIteratorTest extends TestCase {
    int[] docsA = new int[]{5, 10, 15, 20};
    double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
    int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
    double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
    int[] docsTogether = new int[]{10, 20};
    double[] scoresTogether = new double[]{12, 24};

    public FilteredCombinationIteratorTest(String testName) {
        super(testName);
    }

    public void testNextCandidate() throws IOException {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = { one, two };

        Parameters filterParameters = new Parameters();
        ScoreCombinationIterator instance = new FilteredCombinationIterator(filterParameters,
                                                                            iterators);

        assertEquals(5, instance.nextCandidate());
        instance.movePast(10);
        assertEquals(15, instance.nextCandidate());
    }

    public void testHasMatch() throws IOException {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = { one, two };

        Parameters anyParameters = new Parameters();
        FilteredCombinationIterator instance = new FilteredCombinationIterator(anyParameters,
                                                                               iterators);

        assertFalse(instance.hasMatch(1));
        assertFalse(instance.hasMatch(2));
        assertFalse(instance.hasMatch(3));
        assertFalse(instance.hasMatch(10));

        instance.moveTo(10);
        assertTrue(instance.hasMatch(10));
    }

    public void testScore() throws IOException {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = { one, two };

        Parameters anyParameters = new Parameters();
        ScoreCombinationIterator instance = new FilteredCombinationIterator(anyParameters, iterators);
        
        for (int i = 0; i < docsTogether.length; i++) {
            assertFalse(instance.isDone());
            instance.moveTo(docsTogether[i]);
            assertTrue(instance.hasMatch(docsTogether[i]));
            assertEquals(scoresTogether[i], instance.score(docsTogether[i], 100));

            instance.movePast(docsTogether[i]);
        }

        assertTrue(instance.isDone());
    }

    public void testMovePast() throws Exception {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = { one, two };

        Parameters anyParameters = new Parameters();
        ScoreCombinationIterator instance = new FilteredCombinationIterator(anyParameters, iterators);

        instance.movePast(5);
        assertEquals(10, instance.nextCandidate());
    }

    public void testMoveTo() throws Exception {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = { one, two };

        Parameters anyParameters = new Parameters();
        ScoreCombinationIterator instance = new FilteredCombinationIterator(anyParameters, iterators);

        instance.moveTo(5);
        assertEquals(6, instance.nextCandidate());
    }
}
