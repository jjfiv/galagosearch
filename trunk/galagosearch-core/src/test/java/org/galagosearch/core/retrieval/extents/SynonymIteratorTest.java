/*
 * SynonymIteratorTest.java
 * JUnit based test
 *
 * Created on September 14, 2007, 8:58 AM
 */
package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.SynonymIterator;
import java.io.IOException;
import junit.framework.*;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class SynonymIteratorTest extends TestCase {
    public SynonymIteratorTest(String testName) {
        super(testName);
    }

    public void testNoData() throws IOException {
        int[][] dataOne = {};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator[] iters = { one };

        SynonymIterator instance = new SynonymIterator(new Parameters(), iters);
        assertTrue(instance.isDone());
    }

    public void testTwoDocuments() throws IOException {
        int[][] dataOne = {{1, 3}};
        int[][] dataTwo = {{2, 4}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };

        SynonymIterator instance = new SynonymIterator(new Parameters(), iters);
        ExtentArray array = instance.extents();

        assertFalse(instance.isDone());
        assertEquals(1, array.getPosition());
        assertEquals(1, array.getBuffer()[0].document);
        assertEquals(3, array.getBuffer()[0].begin);
        assertEquals(4, array.getBuffer()[0].end);

        instance.nextDocument();
        assertFalse(instance.isDone());
        assertEquals(1, array.getPosition());
        assertEquals(2, array.getBuffer()[0].document);
        assertEquals(4, array.getBuffer()[0].begin);
        assertEquals(5, array.getBuffer()[0].end);

        instance.nextDocument();
        assertTrue(instance.isDone());
    }

    public void testSameDocument() throws IOException {
        int[][] dataOne = {{1, 3}};
        int[][] dataTwo = {{1, 4}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };

        SynonymIterator instance = new SynonymIterator(new Parameters(), iters);
        ExtentArray array = instance.extents();

        assertFalse(instance.isDone());
        assertEquals(array.getPosition(), 2);
        assertEquals(array.getBuffer()[0].document, 1);
        assertEquals(array.getBuffer()[0].begin, 3);
        assertEquals(array.getBuffer()[0].end, 4);

        assertEquals(array.getBuffer()[1].document, 1);
        assertEquals(array.getBuffer()[1].begin, 4);
        assertEquals(array.getBuffer()[1].end, 5);

        instance.nextDocument();
        assertTrue(instance.isDone());
    }
}
