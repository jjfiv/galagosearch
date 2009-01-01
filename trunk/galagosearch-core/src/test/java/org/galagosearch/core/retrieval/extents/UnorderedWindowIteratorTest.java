
package org.galagosearch.core.retrieval.extents;

import org.galagosearch.core.retrieval.structured.UnorderedWindowIterator;
import org.galagosearch.tupleflow.Parameters;
import java.io.IOException;
import junit.framework.*;
import org.galagosearch.core.util.ExtentArray;

/**
 *
 * @author trevor
 */
public class UnorderedWindowIteratorTest extends TestCase {
    public UnorderedWindowIteratorTest(String testName) {
        super(testName);
    }

    public void testPhrase() throws IOException {
        int[][] dataOne = {{1, 3}};
        int[][] dataTwo = {{1, 4}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };
        
        Parameters twoParam = new Parameters();
        twoParam.add("width", "2");
        UnorderedWindowIterator instance = new UnorderedWindowIterator(twoParam, iters);
        ExtentArray array = instance.extents();
        assertFalse(instance.isDone());

        assertEquals(array.getPosition(), 1);
        assertEquals(array.getBuffer()[0].document, 1);
        assertEquals(array.getBuffer()[0].begin, 3);
        assertEquals(array.getBuffer()[0].end, 5);

        instance.nextDocument();
        assertTrue(instance.isDone());
    }

    public void testUnordered() throws IOException {
        int[][] dataOne = {{1, 3}};
        int[][] dataTwo = {{1, 4}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };

        Parameters twoParam = new Parameters();
        twoParam.add("width", "2");
        UnorderedWindowIterator instance = new UnorderedWindowIterator(twoParam, iters);
        ExtentArray array = instance.extents();
        assertFalse(instance.isDone());

        assertEquals(array.getPosition(), 1);
        assertEquals(array.getBuffer()[0].document, 1);
        assertEquals(array.getBuffer()[0].begin, 3);
        assertEquals(array.getBuffer()[0].end, 5);

        instance.nextDocument();
        assertTrue(instance.isDone());
    }

    public void testDifferentDocuments() throws IOException {
        int[][] dataOne = {{2, 3}};
        int[][] dataTwo = {{1, 4}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };

        Parameters twoParam = new Parameters();
        twoParam.add("width", "2");

        UnorderedWindowIterator instance = new UnorderedWindowIterator(twoParam, iters);
        ExtentArray array = instance.extents();
        assertEquals(0, array.getPosition());
        assertTrue(instance.isDone());
    }

    public void testMultipleDocuments() throws IOException {
        int[][] dataOne = {{1, 3}, {2, 5}, {5, 11}};
        int[][] dataTwo = {{1, 4}, {3, 8}, {5, 9}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };

        Parameters fiveParam = new Parameters();
        fiveParam.add("width", "5");

        UnorderedWindowIterator instance = new UnorderedWindowIterator(fiveParam, iters);
        ExtentArray array = instance.extents();
        assertFalse(instance.isDone());

        assertEquals(array.getPosition(), 1);
        assertEquals(array.getBuffer()[0].document, 1);
        assertEquals(array.getBuffer()[0].begin, 3);
        assertEquals(array.getBuffer()[0].end, 5);

        instance.nextDocument();
        assertFalse(instance.isDone());

        assertEquals(array.getPosition(), 1);
        assertEquals(array.getBuffer()[0].document, 5);
        assertEquals(array.getBuffer()[0].begin, 9);
        assertEquals(array.getBuffer()[0].end, 12);

        instance.nextDocument();
        assertTrue(instance.isDone());
    }
}
