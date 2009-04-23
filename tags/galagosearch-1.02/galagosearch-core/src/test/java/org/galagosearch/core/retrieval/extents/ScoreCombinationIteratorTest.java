/*
 * ScoreCombinationIteratorTest.java
 * JUnit based test
 *
 * Created on October 9, 2007, 2:43 PM
 */
package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.ScoreCombinationIterator;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.tupleflow.Parameters;
import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.structured.ScoreIterator;

/**
 *
 * @author trevor
 */
public class ScoreCombinationIteratorTest extends TestCase {
    int[] docsA = new int[]{5, 10, 15, 20};
    double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
    int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
    double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
    int[] docsTogether = new int[]{2, 4, 5, 6, 8, 10, 12, 14, 15, 16, 18, 20};
    double[] scoresTogether = new double[]{2, 4, 1, 6, 8, 12, 12, 14, 3, 16, 18, 24};

    public ScoreCombinationIteratorTest(String testName) {
        super(testName);
    }

    public ScoreCombinationIterator mockIterator(Parameters parameters, ScoreIterator[] iterators) {
        return new ScoreCombinationIterator(parameters, iterators) {
            public int nextCandidate() {
                throw new UnsupportedOperationException("Abstract method.");
            }

            public boolean hasMatch(int document) {
                throw new UnsupportedOperationException("Abstract method.");
            }

            public boolean isDone() {
                throw new UnsupportedOperationException("Abstract method.");
            }
        };
    }

    public void testScore() throws IOException {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = {one, two};

        Parameters anyParameters = new Parameters();
        ScoreCombinationIterator instance = mockIterator(anyParameters, iterators);

        for (int i = 0; i < 12; i++) {
            assertEquals(scoresTogether[i], instance.score(docsTogether[i], 100));
            instance.movePast(docsTogether[i]);
        }
    }

    public void testMovePast() throws Exception {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = {one, two};

        Parameters anyParameters = new Parameters();
        ScoreCombinationIterator instance = mockIterator(anyParameters, iterators);
        instance.movePast(5);
    }

    public void testMoveTo() throws Exception {
        FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
        FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
        FakeScoreIterator[] iterators = {one, two};

        Parameters anyParameters = new Parameters();
        ScoreCombinationIterator instance = mockIterator(anyParameters, iterators);

        instance.moveTo(5);
    }
}
