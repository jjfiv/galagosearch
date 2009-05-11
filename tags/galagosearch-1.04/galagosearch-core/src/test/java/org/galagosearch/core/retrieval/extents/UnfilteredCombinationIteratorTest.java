/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.ScoreCombinationIterator;
import org.galagosearch.core.retrieval.structured.UnfilteredCombinationIterator;
import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class UnfilteredCombinationIteratorTest extends TestCase {
    public UnfilteredCombinationIteratorTest(String testName) {
        super(testName);
    }
    int[] docsA = new int[]{5, 10, 15, 20};
    double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
    int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
    double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
    int[] docsTogether = new int[]{2, 4, 5, 6, 8, 10, 12, 14, 15, 16, 18, 20};
    double[] scoresTogether = new double[]{2, 4, 1, 6, 8, 12, 12, 14, 3, 16, 18, 24};

    public void testNextCandidateAny() throws IOException {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = { one, two };

        Parameters anyParameters = new Parameters();
        UnfilteredCombinationIterator instance = new UnfilteredCombinationIterator(anyParameters,
                                                                                   iterators);

        assertEquals(2, instance.nextCandidate());
        instance.movePast(2);
        assertEquals(4, instance.nextCandidate());
        instance.movePast(4);
        assertEquals(5, instance.nextCandidate());
        instance.movePast(5);
        assertEquals(6, instance.nextCandidate());
    }

    public void testHasMatch() {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = { one, two };

        Parameters anyParameters = new Parameters();
        ScoreCombinationIterator instance = new UnfilteredCombinationIterator(anyParameters,
                                                                              iterators);

        assertFalse(instance.hasMatch(1));
        assertTrue(instance.hasMatch(2));
        assertFalse(instance.hasMatch(3));
    }

    public void testScore() throws IOException {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = { one, two };

        Parameters anyParameters = new Parameters();
        UnfilteredCombinationIterator instance = new UnfilteredCombinationIterator(anyParameters,
                                                                                   iterators);

        for (int i = 0; i < 12; i++) {
            assertFalse(instance.isDone());
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
        UnfilteredCombinationIterator instance = new UnfilteredCombinationIterator(anyParameters,
                                                                                   iterators);

        instance.movePast(5);
        assertEquals(6, instance.nextCandidate());
    }

    public void testMoveTo() throws Exception {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = { one, two };

        Parameters anyParameters = new Parameters();
        UnfilteredCombinationIterator instance = new UnfilteredCombinationIterator(anyParameters,
                                                                                   iterators);

        instance.moveTo(5);
        assertEquals(5, instance.nextCandidate());
    }
}
