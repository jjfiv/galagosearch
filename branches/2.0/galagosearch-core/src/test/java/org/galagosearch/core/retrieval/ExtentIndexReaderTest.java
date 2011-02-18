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
    File skipPath;

    public ExtentIndexReaderTest(String testName) {
        super(testName);
    }

    @Override
    public void setUp() throws Exception {
        // make a spot for the index
        tempPath = File.createTempFile("galago-test-index", null);
        tempPath.delete();

        skipPath = Utility.createTemporary();
        skipPath.delete();

        Parameters p = new Parameters();
        p.add("filename", tempPath.toString());

        ExtentIndexWriter writer =
                new ExtentIndexWriter(new org.galagosearch.tupleflow.FakeParameters(p));

        writer.processExtentName(Utility.fromString("title"));
        writer.processNumber(1);
        writer.processBegin(2);
        writer.processTuple(3);
        writer.processBegin(10);
        writer.processTuple(11);

        writer.processNumber(9);
        writer.processBegin(5);
        writer.processTuple(10);

        writer.processExtentName(Utility.fromString("z"));
        writer.processNumber(15);
        writer.processBegin(9);
        writer.processTuple(11);

        writer.close();
    }

    @Override
    public void tearDown() throws Exception {
        tempPath.delete();
        if (skipPath != null) {
            skipPath.delete();
        }
    }

    public void testReadTitle() throws Exception {
        ExtentIndexReader reader = new ExtentIndexReader(new IndexReader(tempPath.toString()));
        ExtentIndexReader.Iterator extents = reader.getExtents("title");

        assertFalse(extents.isDone());

        ExtentArray e = extents.extents();
        assertEquals(2, e.getPositionCount());
        ExtentArrayIterator iter = new ExtentArrayIterator(e);
        assertFalse(iter.isDone());

        assertEquals(1, extents.identifier());

        assertEquals(2, iter.current().begin);
        assertEquals(3, iter.current().end);

        iter.next();
        assertFalse(iter.isDone());

        assertEquals(10, iter.current().begin);
        assertEquals(11, iter.current().end);

        iter.next();
        assertTrue(iter.isDone());

        extents.nextEntry();
        assertFalse(extents.isDone());

        e = extents.extents();
        iter = new ExtentArrayIterator(e);

        assertEquals(9, extents.identifier());

        assertEquals(5, iter.current().begin);
        assertEquals(10, iter.current().end);

        extents.nextEntry();
        assertTrue(extents.isDone());

        reader.close();
    }

    public void testReadZ() throws Exception {
        ExtentIndexReader reader = new ExtentIndexReader(new IndexReader(tempPath.toString()));
        ExtentIndexReader.Iterator extents = reader.getExtents("z");

        assertFalse(extents.isDone());

        ExtentArray e = extents.extents();
        ExtentArrayIterator iter = new ExtentArrayIterator(e);

        assertEquals(15, extents.identifier());

        assertEquals(9, iter.current().begin);
        assertEquals(11, iter.current().end);

        extents.nextEntry();
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

    public void testSkipList() throws Exception {
        Parameters p = new Parameters();
        p.add("filename", tempPath.toString());
        p.add("skipDistance", "10");

        ExtentIndexWriter writer =
                new ExtentIndexWriter(new org.galagosearch.tupleflow.FakeParameters(p));

        writer.processExtentName(Utility.fromString("skippy"));
        for (int docid = 1; docid < 1000; docid += 3) {
            writer.processNumber(docid);
            for (int begin = 5; begin < (20 + (docid / 5)); begin += 4) {
                writer.processBegin(begin);
                writer.processTuple(begin + 2);
            }
        }
        writer.close();

        ExtentIndexReader reader = new ExtentIndexReader(new IndexReader(tempPath.toString()));
        ExtentIndexReader.Iterator extents = reader.getExtents("skippy");

        assertFalse(extents.isDone());
        assertFalse(extents.skipToDocument(453));
        assertEquals(454, extents.identifier());
        extents.nextEntry();
        assertEquals(457, extents.identifier());
        assertEquals(27, extents.count());
        ExtentArray ea = extents.extents();
        ExtentArrayIterator eait = new ExtentArrayIterator(ea);
        int begin = 5;
        while (!eait.isDone()) {
            assertEquals(begin, eait.current().begin);
            assertEquals(begin + 2, eait.current().end);
            begin += 4;
            eait.next();
        }
        extents.moveTo(1299);
        assertFalse(extents.hasMatch(1299));
        extents.movePast(2100);
        assertTrue(extents.isDone());
        reader.close();

        skipPath.delete();
        skipPath = null;
    }
}
