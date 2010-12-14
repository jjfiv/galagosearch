/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.scoring;

import junit.framework.TestCase;
import org.galagosearch.core.retrieval.extents.FakeExtentIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
public class BM25ScorerTest extends TestCase {

    int[][] dummy = {};
    int[][] five = {
        {1,2,3},
        {5,10,60},
        {1, 90},
        {4, 78, 2343},
        {100}};
    
    FakeExtentIterator iterator;

    public void testScorer() throws Exception {
        // start with as many defaults as possible and
        // a fake iterator
        Parameters p = new Parameters();
        p.set("collectionLength", "5000");
        p.set("documentCount", "100");
        FakeExtentIterator iterator = new FakeExtentIterator(dummy);

        BM25Scorer scorer = new BM25Scorer(p, iterator);
        assertEquals(0.75, scorer.b);
        assertEquals(1.2, scorer.k);
        assertEquals(50.0, scorer.avgDocLength);
        assertEquals(5.30330, scorer.idf, 0.0001);
        assertEquals(8.21639,scorer.score(5, 100), 0.0001);

        // Add in an iterator w/ some docs
        iterator = new FakeExtentIterator(five);
        scorer = new BM25Scorer(p, iterator);
        assertEquals(0.75, scorer.b);
        assertEquals(1.2, scorer.k);
        assertEquals(50.0, scorer.avgDocLength);
        assertEquals(2.85438, scorer.idf, 0.0001);
        assertEquals(5.44870, scorer.score(12,85), 0.0001);

        // explicitly set everything
        p.set("b","0.3");
        p.set("k","2.0");
        p.set("df","20");
        scorer = new BM25Scorer(p, null);
        assertEquals(0.3, scorer.b);
        assertEquals(2.0, scorer.k);
        assertEquals(50.0, scorer.avgDocLength);
        assertEquals(1.36783, scorer.idf, 0.0001);
        assertEquals(3.27407, scorer.score(15,200), 0.0001);
    }
}
