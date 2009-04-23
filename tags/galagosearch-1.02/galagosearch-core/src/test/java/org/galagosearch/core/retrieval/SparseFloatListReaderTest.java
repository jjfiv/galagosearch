/*
 * SparseFloatListReaderTest.java
 * JUnit based test
 *
 * Created on October 9, 2007, 1:00 PM
 */
package org.galagosearch.core.retrieval;

import org.galagosearch.core.index.SparseFloatListReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.core.index.SparseFloatListWriter;
import org.galagosearch.tupleflow.FakeParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.TupleFlowParameters;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class SparseFloatListReaderTest extends TestCase {
    private File tempPath;
    int[] aDocs = new int[]{5, 6};
    float[] aScores = new float[]{0.5f, 0.7f};
    int[] bDocs = new int[]{9, 11, 13};
    float[] bScores = new float[]{0.1f, 0.2f, 0.3f};

    public SparseFloatListReaderTest(String testName) {
        super(testName);
    }

    @Override
    public void setUp() throws IOException {
        // make a spot for the index
        tempPath = File.createTempFile("galago-test-index", null);
        tempPath.delete();

        Parameters p = new Parameters();
        p.add("filename", tempPath.toString());

        TupleFlowParameters parameters = new FakeParameters(p);
        SparseFloatListWriter writer = new SparseFloatListWriter(parameters);

        writer.processWord(Utility.makeBytes("a"));

        for (int i = 0; i < aDocs.length; i++) {
            writer.processNumber(aDocs[i]);
            writer.processTuple(aScores[i]);
        }

        writer.processWord(Utility.makeBytes("b"));

        for (int i = 0; i < bDocs.length; i++) {
            writer.processNumber(bDocs[i]);
            writer.processTuple(bScores[i]);
        }

        writer.close();
    }

    @Override
    public void tearDown() throws IOException {
        tempPath.delete();
    }

    public void testA() throws Exception {
        SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
        SparseFloatListReader.Iterator iter = instance.getScores("a");
        assertFalse(iter.isDone());
        int i = 0;

        for (i = 0; !iter.isDone(); i++) {
            assertEquals(aDocs[i], iter.nextCandidate());
            assertEquals(aScores[i], iter.score(aDocs[i], 10), 0.0001);
            assertTrue(iter.hasMatch(aDocs[i]));

            iter.movePast(aDocs[i]);
        }

        assertEquals(aDocs.length, i);
        assertTrue(iter.isDone());
    }

    public void testB() throws Exception {
        SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
        SparseFloatListReader.Iterator iter = instance.getScores("b");
        int i;

        assertFalse(iter.isDone());

        for (i = 0; !iter.isDone(); i++) {
            assertEquals(bDocs[i], iter.nextCandidate());
            assertEquals(bScores[i], iter.score(bDocs[i], 10), 0.0001);
            assertTrue(iter.hasMatch(bDocs[i]));

            iter.movePast(bDocs[i]);
        }

        assertEquals(bDocs.length, i);
        assertTrue(iter.isDone());
    }

    public void testIterator() throws Exception {
        SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
        SparseFloatListReader.Iterator iter = instance.getIterator();
        String term = iter.currentTerm();

        assertEquals(term, "a");
        assertFalse(iter.isDone());

        for (int i = 0; !iter.isDone(); i++) {
            assertEquals(iter.nextCandidate(), aDocs[i]);
            assertEquals(iter.score(aDocs[i], 10), aScores[i], 0.0001);
            assertTrue(iter.hasMatch(aDocs[i]));

            iter.movePast(aDocs[i]);
        }

        assertTrue(iter.nextTerm());
        term = iter.currentTerm();
        assertEquals(term, "b");
        assertFalse(iter.isDone());

        for (int i = 0; !iter.isDone(); i++) {
            assertEquals(iter.nextCandidate(), bDocs[i]);
            assertEquals(iter.score(bDocs[i], 10), bScores[i], 0.0001);
            assertTrue(iter.hasMatch(bDocs[i]));

            iter.movePast(bDocs[i]);
        }

        assertFalse(iter.nextTerm());
    }
}
