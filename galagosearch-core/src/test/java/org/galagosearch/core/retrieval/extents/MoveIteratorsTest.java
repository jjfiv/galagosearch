/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.ExtentIterator;
import org.galagosearch.core.retrieval.structured.MoveIterators;
import java.io.IOException;
import java.util.ArrayList;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class MoveIteratorsTest extends TestCase {
    private ExtentIterator[] iterators;
    
    public MoveIteratorsTest(String testName) {
        super(testName);
    }            

    @Override
    protected void setUp() throws Exception {
        ExtentIterator one = new FakeExtentIterator(new int[][] { {2,1}, {3,1}, {5,1} });
        ExtentIterator two = new FakeExtentIterator(new int[][] { {3,1}, {6,1}, {7,1} });
        iterators = new ExtentIterator[] { one, two };
    }
    
    /**
     * Test of moveAllToSameDocument method, of class MoveIterators.
     */
    public void testMoveAllToSameDocument() throws Exception {
        assertEquals(3, MoveIterators.moveAllToSameDocument(iterators));
        iterators[0].nextDocument();
        assertEquals(Integer.MAX_VALUE, MoveIterators.moveAllToSameDocument(iterators));
    }

    /**
     * Test of allSameDocument method, of class MoveIterators.
     */
    public void testAllSameDocument() throws IOException {
        assertFalse(MoveIterators.allSameDocument(iterators));
        iterators[0].nextDocument();
        assertTrue(MoveIterators.allSameDocument(iterators));
    }

    /**
     * Test of findMaximumDocument method, of class MoveIterators.
     */
    public void testFindMaximumDocument() throws IOException {
        assertEquals(3, MoveIterators.findMaximumDocument(iterators));
        iterators[0].nextDocument();
        assertEquals(3, MoveIterators.findMaximumDocument(iterators));
        iterators[1].nextDocument();
        assertEquals(6, MoveIterators.findMaximumDocument(iterators));
        iterators[1].nextDocument();
        assertEquals(7, MoveIterators.findMaximumDocument(iterators));
        iterators[1].nextDocument();
        assertEquals(Integer.MAX_VALUE, MoveIterators.findMaximumDocument(iterators));
    }
}
