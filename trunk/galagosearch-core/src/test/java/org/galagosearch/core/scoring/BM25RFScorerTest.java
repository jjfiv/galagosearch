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
public class BM25RFScorerTest extends TestCase {

    int[][] dummy = {};
    int[][] five = {
        {1,2,3},
        {5,10,60},
        {1, 90},
        {4, 78, 2343},
        {100}};
    
    FakeExtentIterator iterator;

    public void testScorer() throws Exception {
        iterator = new FakeExtentIterator(dummy);

        // test empty
        Parameters parameters = new Parameters();
        BM25RFScorer scorer = new BM25RFScorer(parameters, iterator);
        assertEquals(0.0,scorer.score(1,1));
        assertEquals(0.0,scorer.score(200,957));

        // set some values
        iterator = new FakeExtentIterator(five);
        parameters.add("rt", "3");
        parameters.add("R", "10");
        parameters.add("documentCount", "1000");
        scorer = new BM25RFScorer(parameters, iterator);
        assertEquals(1.72186,scorer.score(1,1), 0.001);
        assertEquals(1.72186,scorer.score(1,234565), 0.001);

        // Fill in ft
        parameters.add("ft", "5");
        scorer = new BM25RFScorer(parameters, null);
        assertEquals(1.72186,scorer.score(1,1), 0.001);
        assertEquals(1.72186,scorer.score(9,9), 0.001);
    }
}
