/*
 * ExtentIndexReaderTest.java
 * JUnit based test
 *
 * Created on October 5, 2007, 4:36 PM
 */
package org.galagosearch.core.retrieval;

import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.index.ExtentIndexReader;
import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.retrieval.structured.ExtentArrayIterator;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.core.util.ExtentArray;
import java.io.File;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class ExtentIndexReaderTest extends TestCase {
    File tempPath;

    public ExtentIndexReaderTest(String testName) {
        super(testName);
    }

    @Override
    public void setUp() throws Exception {
        // make a spot for the index
        tempPath = File.createTempFile("galago-test-index", null);
        tempPath.delete();

        Parameters p = new Parameters();
        p.add("filename", tempPath.toString());

        ExtentIndexWriter writer =
                new ExtentIndexWriter(new org.galagosearch.tupleflow.FakeParameters(p));

        writer.processExtentName(Utility.makeBytes("title"));
        writer.processNumber(1);
        writer.processBegin(2);
        writer.processTuple(3);
        writer.processBegin(10);
        writer.processTuple(11);

        writer.processNumber(9);
        writer.processBegin(5);
        writer.processTuple(10);

        writer.processExtentName(Utility.makeBytes("z"));
        writer.processNumber(15);
        writer.processBegin(9);
        writer.processTuple(11);

        writer.close();
    }

    @Override
    public void tearDown() throws Exception {
        tempPath.delete();
    }

    public void testReadTitle() throws Exception {
        ExtentIndexReader reader = new ExtentIndexReader(new IndexReader(tempPath.toString()));
        ExtentIndexReader.Iterator extents = reader.getExtents("title");

        assertFalse(extents.isDone());

        ExtentArray e = extents.extents();
        ExtentArrayIterator iter = new ExtentArrayIterator(e);
        assertFalse(iter.isDone());

        assertEquals(1, extents.document());

        assertEquals(2, iter.current().begin);
        assertEquals(3, iter.current().end);

        iter.next();
        assertFalse(iter.isDone());

        assertEquals(10, iter.current().begin);
        assertEquals(11, iter.current().end);

        iter.next();
        assertTrue(iter.isDone());

        extents.nextDocument();
        assertFalse(extents.isDone());

        e = extents.extents();
        iter = new ExtentArrayIterator(e);

        assertEquals(9, extents.document());

        assertEquals(5, iter.current().begin);
        assertEquals(10, iter.current().end);

        extents.nextDocument();
        assertTrue(extents.isDone());

        reader.close();
    }

    public void testReadZ() throws Exception {
        ExtentIndexReader reader = new ExtentIndexReader(new IndexReader(tempPath.toString()));
        ExtentIndexReader.Iterator extents = reader.getExtents("z");

        assertFalse(extents.isDone());

        ExtentArray e = extents.extents();
        ExtentArrayIterator iter = new ExtentArrayIterator(e);

        assertEquals(15, extents.document());

        assertEquals(9, iter.current().begin);
        assertEquals(11, iter.current().end);

        extents.nextDocument();
        assertTrue(extents.isDone());

        reader.close();
    }

    public void testSimpleSkipTitle() throws Exception {
        ExtentIndexReader reader = new ExtentIndexReader(new IndexReader(tempPath.toString()));
        ExtentIndexReader.Iterator extents = reader.getExtents("title");

        assertFalse(extents.isDone());
        extents.skipToDocument(10);
        assertTrue(extents.isDone());

        reader.close();
    }
}
