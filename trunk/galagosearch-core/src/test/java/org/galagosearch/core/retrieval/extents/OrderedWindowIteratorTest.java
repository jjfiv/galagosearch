/*
 * OrderedWindowIteratorTest.java
 * JUnit based test
 *
 * Created on September 13, 2007, 7:00 PM
 */
package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.OrderedWindowIterator;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.core.util.ExtentArray;
import java.io.IOException;
import junit.framework.*;

/**
 *
 * @author trevor
 */
public class OrderedWindowIteratorTest extends TestCase {
    public OrderedWindowIteratorTest(String testName) {
        super(testName);
    }

    public void testPhrase() throws IOException {
        int[][] dataOne = {{1, 3}};
        int[][] dataTwo = {{1, 4}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };

        Parameters oneParam = new Parameters();
        oneParam.add("width", "1");
        OrderedWindowIterator instance = new OrderedWindowIterator(oneParam, iters);

        ExtentArray array = instance.extents();

        assertEquals(array.getPosition(), 1);
        assertEquals(array.getBuffer()[0].document, 1);
        assertEquals(array.getBuffer()[0].begin, 3);
        assertEquals(array.getBuffer()[0].end, 5);
    }

    public void testWrongOrder() throws IOException {
        int[][] dataOne = {{1, 3}};
        int[][] dataTwo = {{1, 4}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { two, one };

        Parameters oneParam = new Parameters();
        oneParam.add("width", "1");
        OrderedWindowIterator instance = new OrderedWindowIterator(oneParam, iters);

        ExtentArray array = instance.extents();
        assertEquals(0, array.getPosition());
    }
}
