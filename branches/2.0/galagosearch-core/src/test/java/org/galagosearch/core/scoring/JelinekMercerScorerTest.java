package org.galagosearch.core.scoring;

import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.extents.FakeExtentIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class JelinekMercerScorerTest extends TestCase {
    
    public JelinekMercerScorerTest(String testName) {
        super(testName);
    }

    public void testCollectionProbability() throws IOException {
        Parameters p = new Parameters();
        p.set("collectionProbability", "0.5");
        JelinekMercerScorer scorer = new JelinekMercerScorer(p, null);

        assertEquals(0.5, scorer.lambda);
        assertEquals(0.5, scorer.background);

        double score = scorer.score(15, 100);
        assertEquals(-1.12393, score, 0.001);
    }

    public void testSetLambda() throws IOException {
        Parameters p = new Parameters();
        p.set("lambda", "0.2");
        p.set("collectionProbability", "0.5");
        JelinekMercerScorer scorer = new JelinekMercerScorer(p, null);

        assertEquals(0.2, scorer.lambda);
        assertEquals(0.5, scorer.background);

        double score = scorer.score(5, 100);
        assertEquals(-0.89160, score, 0.001);
    }

    public void testCountFromIterator() throws Exception {
        int[][] data = { { 1, 5 } };
        FakeExtentIterator iterator = new FakeExtentIterator(data);

        Parameters p = new Parameters();
        p.set("collectionLength", "256");
        JelinekMercerScorer scorer = new JelinekMercerScorer(p, iterator);

        assertEquals(0.5, scorer.lambda);
        assertEquals(1.0 / 256.0, scorer.background);

        double score = scorer.score(5, 100);
        assertEquals(-3.61366, score, 0.0001);
    }

    public void testCountFromEmptyIterator() throws Exception {
        int[][] data = {};
        FakeExtentIterator iterator = new FakeExtentIterator(data);

        Parameters p = new Parameters();
        p.set("collectionLength", "256");
        JelinekMercerScorer scorer = new JelinekMercerScorer(p, iterator);

        assertEquals(0.5, scorer.lambda);
        assertEquals(0.5 / 256.0, scorer.background);

        double score = scorer.score(5, 100);
        assertEquals(-3.65056, score, 0.0001);
    }
}
