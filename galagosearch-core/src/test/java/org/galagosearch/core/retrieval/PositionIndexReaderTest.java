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

/**
 *
 * @author trevor
 */
public class PositionIndexReaderTest extends TestCase {
    File tempPath;
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

        Parameters p = new Parameters();
        p.add("filename", tempPath.toString());

        PositionIndexWriter writer =
                new PositionIndexWriter(new org.galagosearch.tupleflow.FakeParameters(p));

        writer.processWord(Utility.makeBytes("a"));

        for (int[] doc : dataA) {
            writer.processDocument(doc[0]);

            for (int i = 1; i < doc.length; i++) {
                writer.processPosition(doc[i]);
            }
        }

        writer.processWord(Utility.makeBytes("b"));

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
    }

    public void internalTestIterator(
            PositionIndexReader.Iterator termExtents,
            int[][] data) throws IOException {
        assertNotNull(termExtents);
        assertFalse(termExtents.isDone());

        for (int[] doc : data) {
            assertFalse(termExtents.isDone());
            ExtentArray e = termExtents.extents();
            ExtentArrayIterator iter = new ExtentArrayIterator(e);

            for (int i = 1; i < doc.length; i++) {
                assertFalse(iter.isDone());
                assertEquals(doc[i], iter.current().begin);
                assertEquals(doc[i] + 1, iter.current().end);
                iter.next();
            }

            assertTrue(iter.isDone());
            termExtents.nextDocument();
        }

        assertTrue(termExtents.isDone());
    }

    public void testA() throws Exception {
        PositionIndexReader reader = new PositionIndexReader(tempPath.toString());
        PositionIndexReader.Iterator termExtents = reader.getTermExtents("a");

        internalTestIterator(termExtents, dataA);
        reader.close();
    }

    public void testB() throws Exception {
        PositionIndexReader reader = new PositionIndexReader(tempPath.toString());
        PositionIndexReader.Iterator termExtents = reader.getTermExtents("b");

        internalTestIterator(termExtents, dataB);
        reader.close();
    }
}
