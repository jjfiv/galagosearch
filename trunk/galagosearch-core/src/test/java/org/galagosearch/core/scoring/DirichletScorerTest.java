package org.galagosearch.core.scoring;

import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.extents.FakeExtentIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class DirichletScorerTest extends TestCase {
    
    public DirichletScorerTest(String testName) {
        super(testName);
    }

    public void testCollectionProbability() throws IOException {
        Parameters p = new Parameters();
        p.set("collectionProbability", "0.5");
        DirichletScorer scorer = new DirichletScorer(p, null);

        assertEquals(1500.0, scorer.mu);
        assertEquals(0.5, scorer.background);

        double score = scorer.scoreCount(15, 100);
        assertEquals(-0.73788, score, 0.001);
    }

    public void testSetMu() throws IOException {
        Parameters p = new Parameters();
        p.set("mu", "13");
        p.set("collectionProbability", "0.5");
        DirichletScorer scorer = new DirichletScorer(p, null);

        assertEquals(13.0, scorer.mu);
        assertEquals(0.5, scorer.background);

        double score = scorer.scoreCount(5, 100);
        assertEquals(-2.28504, score, 0.001);
    }

    public void testCountFromIterator() throws Exception {
        int[][] data = { { 1, 5 } };
        FakeExtentIterator iterator = new FakeExtentIterator(data);

        Parameters p = new Parameters();
        p.set("collectionLength", "256");
        DirichletScorer scorer = new DirichletScorer(p, iterator);

        assertEquals(1500.0, scorer.mu);
        assertEquals(1.0 / 256.0, scorer.background);

        double score = scorer.scoreCount(5, 100);
        assertEquals(-4.99273, score, 0.0001);
    }

    public void testCountFromEmptyIterator() throws Exception {
        int[][] data = {};
        FakeExtentIterator iterator = new FakeExtentIterator(data);

        Parameters p = new Parameters();
        p.set("collectionLength", "256");
        DirichletScorer scorer = new DirichletScorer(p, iterator);

        assertEquals(1500.0, scorer.mu);
        assertEquals(0.5 / 256.0, scorer.background);

        double score = scorer.scoreCount(5, 100);
        assertEquals(-5.307145, score, 0.0001);
    }
}
