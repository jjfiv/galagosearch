/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.extents;

import junit.framework.TestCase;
import org.galagosearch.core.retrieval.structured.DocumentContext;
import org.galagosearch.core.retrieval.structured.ScaleIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author marc
 */
public class ScaleIteratorTest extends TestCase {

    int[] docsA = new int[]{5, 10, 15, 20};
    double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
    int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
    double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};

    public ScaleIteratorTest(String testName) {
        super(testName);
    }

    public void testA() throws Exception {
        FakeScoreIterator inner = new FakeScoreIterator(docsA, scoresA);
        Parameters dummyParameters = new Parameters();
        ScaleIterator iterator = new ScaleIterator(dummyParameters, inner);
        assertFalse(iterator.isDone());
        assertTrue(iterator.hasMatch(docsA[0]));
        for (int i = 0; i < docsA.length; i++) {
            assertEquals(docsA[i], iterator.currentCandidate());
            assertEquals(scoresA[i], iterator.score(new DocumentContext(docsA[i], 100)));
            iterator.movePast(docsA[i]);
        }
        assertTrue(iterator.isDone());
        iterator.reset();
        assertTrue(iterator.hasMatch(docsA[0]));
    }

    public void testB() throws Exception {
       double weight = 0.5;
       FakeScoreIterator inner = new FakeScoreIterator(docsB, scoresB);
       Parameters weightedParameters = new Parameters();
       weightedParameters.set("default", Double.toString(weight));
       ScaleIterator iterator = new ScaleIterator(weightedParameters, inner);
        assertFalse(iterator.isDone());
        assertTrue(iterator.hasMatch(docsB[0]));
        for (int i = 0; i < docsB.length; i++) {
            iterator.moveTo(docsB[i]);
            assertEquals(docsB[i], iterator.currentCandidate());
            assertEquals(scoresB[i]*weight, iterator.score(new DocumentContext(docsB[i], 100)));
        }
        iterator.reset();
        assertTrue(iterator.hasMatch(docsB[0]));
    }
}
