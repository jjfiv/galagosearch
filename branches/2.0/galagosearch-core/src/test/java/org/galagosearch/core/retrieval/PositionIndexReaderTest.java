/*
 * PositionIndexReaderTest.java
 * JUnit based test
 *
 * Created on October 5, 2007, 4:38 PM
 */
package org.galagosearch.core.retrieval;

import org.galagosearch.core.index.PositionIndexReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.retrieval.structured.ExtentArrayIterator;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.core.util.ExtentArray;
import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.core.retrieval.structured.Extent;
import org.galagosearch.core.retrieval.structured.ExtentIndexIterator;

/**
 *
 * @author trevor
 */
public class PositionIndexReaderTest extends TestCase {

    File tempPath;
    File skipPath = null;
    static int[][] dataA = {
        {5, 7, 9},
        {19, 27, 300}
    };
    static int[][] dataB = {
        {149, 15500, 30319},
        {555555, 2}
    };

    public PositionIndexReaderTest(String testName) {
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

        PositionIndexWriter writer =
                new PositionIndexWriter(new org.galagosearch.tupleflow.FakeParameters(p));

        writer.processWord(Utility.fromString("a"));

        for (int[] doc : dataA) {
            writer.processDocument(doc[0]);

            for (int i = 1; i < doc.length; i++) {
                writer.processPosition(doc[i]);
            }
        }

        writer.processWord(Utility.fromString("b"));

        for (int[] doc : dataB) {
            writer.processDocument(doc[0]);

            for (int i = 1; i < doc.length; i++) {
                writer.processPosition(doc[i]);
            }
        }

        writer.close();
    }

    @Override
    public void tearDown() throws Exception {
        tempPath.delete();
        if (skipPath != null) skipPath.delete();
    }

    public void internalTestIterator(
            ExtentIndexIterator termExtents,
            int[][] data) throws IOException {
        assertNotNull(termExtents);
        assertFalse(termExtents.isDone());
        assertEquals(data.length, ((PositionIndexReader.Iterator) termExtents).totalDocuments());
        int totalPositions = 0;
        for (int[] doc : data) {
            assertFalse(termExtents.isDone());
            ExtentArray e = termExtents.extents();
            ExtentArrayIterator iter = new ExtentArrayIterator(e);
            totalPositions += (doc.length - 1); // first entry in doc array is docid
            for (int i = 1; i < doc.length; i++) {
                assertFalse(iter.isDone());
                assertEquals(doc[i], iter.current().begin);
                assertEquals(doc[i] + 1, iter.current().end);
                iter.next();
            }
            assertTrue(iter.isDone());
            termExtents.nextEntry();
        }
        assertEquals(((PositionIndexReader.Iterator) termExtents).totalPositions(), totalPositions);
        assertTrue(termExtents.isDone());
    }

    public void testA() throws Exception {
        PositionIndexReader reader = new PositionIndexReader(tempPath.toString());
        ExtentIndexIterator termExtents = reader.getTermExtents("a");

        internalTestIterator(termExtents, dataA);
        assertEquals(2, reader.documentCount("a"));
        assertEquals(4, reader.termCount("a"));
        reader.close();
    }

    public void testB() throws Exception {
        PositionIndexReader reader = new PositionIndexReader(tempPath.toString());
        ExtentIndexIterator termExtents = reader.getTermExtents("b");

        internalTestIterator(termExtents, dataB);
        assertEquals(2, reader.documentCount("b"));
        assertEquals(3, reader.termCount("b"));
        reader.close();
    }

    public void testSkipLists() throws Exception {
        // internally fill the skip file
        Parameters p = new Parameters();
        p.add("filename", skipPath.toString());
        p.add("skipping", "true");
        p.add("skipDistance", "20");
        p.add("skipResetDistance", "5");

        PositionIndexWriter writer =
                new PositionIndexWriter(new org.galagosearch.tupleflow.FakeParameters(p));

        writer.processWord(Utility.fromString("a"));
        for (int docid = 1; docid < 5000; docid += 3) {
            writer.processDocument(docid);
            for (int pos = 1; pos < ((docid/50)+2); pos++) {
                writer.processPosition(pos);
            }
        }
        writer.close();

        // Now read it
        PositionIndexReader reader = new PositionIndexReader(skipPath.toString());
        ExtentIndexIterator termExtents = reader.getTermExtents("a");
        assertEquals("a", termExtents.getKey());

        // Read first document
        assertEquals(1, termExtents.document());
        assertEquals(1, termExtents.count());

        termExtents.moveTo(7);
        assertTrue(termExtents.hasMatch(7));

        // Now move to a doc, but not one we have
        termExtents.moveTo(90);
        assertFalse(termExtents.hasMatch(90));

        // Now move forward one
        termExtents.nextEntry();
        assertEquals(94, termExtents.document());
        assertEquals(2, termExtents.count());

        // One more time, then we read extents
        termExtents.movePast(2543);
        assertEquals(2545, termExtents.document());
        assertEquals(51, termExtents.count());
        ExtentArray ea = termExtents.extents();
        Extent[] buffer = ea.getBuffer();
        assertEquals(51, ea.getPositionCount());
        for (int i = 0; i < ea.getPositionCount(); i++) {
            assertEquals(2545, buffer[i].document);
            assertEquals(i+1, buffer[i].begin);
        }
        termExtents.skipToDocument(10005);
        assertFalse(termExtents.hasMatch(10005));
        assertTrue(termExtents.isDone());

        skipPath.delete();
        skipPath = null;
    }

    public void testCountIterator() throws Exception {
        PositionIndexReader reader = new PositionIndexReader(tempPath.toString());
        ExtentIndexIterator termCounts = reader.getTermCounts("b");

        assertEquals(dataB[0][0], termCounts.document());
        assertEquals(dataB[0].length-1, termCounts.count());
        termCounts.nextEntry();

        assertEquals(dataB[1][0], termCounts.document());
        assertEquals(dataB[1].length-1, termCounts.count());

        assertEquals(2, reader.documentCount("b"));
        assertEquals(3, reader.termCount("b"));

        reader.close();
    }
}
