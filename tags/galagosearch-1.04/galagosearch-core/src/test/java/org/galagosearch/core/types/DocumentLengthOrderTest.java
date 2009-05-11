// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.types;

import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.ArrayInput;
import org.galagosearch.tupleflow.ArrayOutput;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.OrderedWriter;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TypeReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class DocumentLengthOrderTest extends TestCase {
    public DocumentLengthOrderTest(String testName) {
        super(testName);
    }
    DocumentLengthWordCount[] dlwcObjects = {
        new DocumentLengthWordCount("a", 1, "b", 2),
        new DocumentLengthWordCount("a", 1, "c", 3),
        new DocumentLengthWordCount("a", 2, "c", 3),
        new DocumentLengthWordCount("b", 1, "c", 3)
    };

    public void testHash() {
        DocumentLengthWordCount one = new DocumentLengthWordCount("a", 1, "b", 2);
        DocumentLengthWordCount two = new DocumentLengthWordCount("a", 1, "c", 3);
        DocumentLengthWordCount three = new DocumentLengthWordCount("b", 1, "c", 3);
        DocumentLengthWordCount four = new DocumentLengthWordCount("a", 2, "c", 3);
        DocumentLengthWordCount.DocumentLengthOrder instance = new DocumentLengthWordCount.DocumentLengthOrder();

        int expResult = 0;
        int oneResult = instance.hash(one);
        int twoResult = instance.hash(two);
        int threeResult = instance.hash(three);
        int fourResult = instance.hash(four);
        assertEquals(oneResult, twoResult);
        assertTrue(twoResult != threeResult);
        assertTrue(twoResult != fourResult);
    }

    public void testGreaterThan() {
        DocumentLengthWordCount.DocumentLengthOrder instance = new DocumentLengthWordCount.DocumentLengthOrder();

        DocumentLengthWordCount one = new DocumentLengthWordCount("a", 1, "b", 2);
        DocumentLengthWordCount two = new DocumentLengthWordCount("a", 1, "c", 3);
        DocumentLengthWordCount three = new DocumentLengthWordCount("b", 1, "c", 3);
        DocumentLengthWordCount four = new DocumentLengthWordCount("a", 2, "c", 3);
        Comparator<DocumentLengthWordCount> result = instance.greaterThan();

        assertEquals(result.compare(one, two), 0);
        assertTrue(result.compare(one, three) > 0);
        assertTrue(result.compare(one, four) > 0);
    }

    public void testLessThan() {
        DocumentLengthWordCount.DocumentLengthOrder instance = new DocumentLengthWordCount.DocumentLengthOrder();
        DocumentLengthWordCount one = new DocumentLengthWordCount("a", 1, "b", 2);
        DocumentLengthWordCount two = new DocumentLengthWordCount("a", 1, "c", 3);
        DocumentLengthWordCount three = new DocumentLengthWordCount("b", 1, "c", 3);
        DocumentLengthWordCount four = new DocumentLengthWordCount("a", 2, "c", 3);
        Comparator<DocumentLengthWordCount> result = instance.lessThan();

        assertEquals(result.compare(one, two), 0);
        assertTrue(result.compare(one, three) < 0);
        assertTrue(result.compare(one, four) < 0);
    }

    public void testOrderedReaderExists() {
        ArrayInput _input = null;
        DocumentLengthWordCount.DocumentLengthOrder instance = new DocumentLengthWordCount.DocumentLengthOrder();

        TypeReader<DocumentLengthWordCount> result = instance.orderedReader(_input);
        assertTrue(result != null);
    }

    public void testOrderedWriterExists() {
        ArrayOutput _output = null;
        DocumentLengthWordCount.DocumentLengthOrder instance = new DocumentLengthWordCount.DocumentLengthOrder();

        OrderedWriter<DocumentLengthWordCount> expResult = null;
        OrderedWriter<DocumentLengthWordCount> result = instance.orderedWriter(_output);
        assertTrue(result != null);
    }

    public void testClone() {
        DocumentLengthWordCount object = new DocumentLengthWordCount("a", 1, "b", 2);
        DocumentLengthWordCount.DocumentLengthOrder instance = new DocumentLengthWordCount.DocumentLengthOrder();

        DocumentLengthWordCount expResult = null;
        DocumentLengthWordCount result = instance.clone(object);
        assertTrue(result.count == object.count);
        assertTrue(result.length == object.length);
        assertTrue(result.document.equals(object.document));
        assertTrue(result.word.equals(object.word));
    }

    public void testGetOrderedClass() {
        DocumentLengthWordCount.DocumentLengthOrder instance = new DocumentLengthWordCount.DocumentLengthOrder();

        Class<DocumentLengthWordCount> result = instance.getOrderedClass();
        assertEquals(result, DocumentLengthWordCount.class);
    }

    public void testReadWriteClass() throws IOException {
        DocumentLengthWordCount.DocumentLengthOrder instance = new DocumentLengthWordCount.DocumentLengthOrder();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ArrayList<DocumentLengthWordCount> tuples = generateSortedTuples(29950);
        ArrayOutput output = new ArrayOutput(new DataOutputStream(stream));

        Processor<DocumentLengthWordCount> writer = instance.orderedWriter(output);

        for (DocumentLengthWordCount d : tuples) {
            writer.process(d);
        }
        writer.close();

        byte[] data = stream.toByteArray();
        ArrayInput input = new ArrayInput(new DataInputStream(new ByteArrayInputStream(data)));
        TypeReader<DocumentLengthWordCount> reader = instance.orderedReader(input);
        int count = 0;

        for (DocumentLengthWordCount d : tuples) {
            DocumentLengthWordCount o = reader.read();
            count++;

            assertEquals(d.count, o.count);
            assertEquals(d.length, o.length);
            assertEquals(d.document, o.document);
            assertEquals(d.word, o.word);
        }
    }

    public static ArrayList<DocumentLengthWordCount> generateSortedTuples(int count) {
        ArrayList<DocumentLengthWordCount> tuples = new ArrayList();
        assert count < 10000000;

        for (int i = 0; i < count; i++) {
            String document = String.format("doc-%09d", i / 20);
            String word = String.format("word-%08d", i);

            tuples.add(new DocumentLengthWordCount(document, i / 5, word, i));
        }

        return tuples;
    }

    public class DocLenWordCountListener implements
            DocumentLengthWordCount.DocumentLengthOrder.ShreddedProcessor, Step {
        DocumentLengthWordCount last = new DocumentLengthWordCount();
        public ArrayList<DocumentLengthWordCount> all = new ArrayList();

        public void processDocument(String document) throws IOException {
            DocumentLengthOrderTest.this.assertTrue(last.document == null || Utility.compare(
                    document, last.document) != 0);
            last.document = document;
            last.length = -450938;
        }

        public void processLength(int length) throws IOException {
            DocumentLengthOrderTest.this.assertTrue(Utility.compare(length, last.length) != 0);
            last.length = length;
        }

        public void processTuple(String word, int count) throws IOException {
            DocumentLengthWordCount n = new DocumentLengthWordCount();

            n.document = last.document;
            n.length = last.length;
            n.count = count;
            n.word = word;

            all.add(n);
        }

        public void close() throws IOException {
        }
    }

    public void testCombination() throws IOException, IncompatibleProcessorException {
        DocumentLengthWordCount.DocumentLengthOrder instance = new DocumentLengthWordCount.DocumentLengthOrder();
        ArrayList<DocumentLengthWordCount> fullList = generateSortedTuples(10000);

        ArrayList<DocumentLengthWordCount> a = new ArrayList();
        a.addAll(fullList.subList(0, 1997));
        a.addAll(fullList.subList(4000, 6000));
        a.addAll(fullList.subList(9000, 10000));

        ArrayList<DocumentLengthWordCount> b = new ArrayList();
        b.addAll(fullList.subList(1997, 4000));
        b.addAll(fullList.subList(6000, 9000));

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ArrayOutput output = new ArrayOutput(new DataOutputStream(stream));
        Processor<DocumentLengthWordCount> writer = instance.orderedWriter(output);

        for (DocumentLengthWordCount dlwc : a) {
            writer.process(dlwc);
        }

        writer.close();
        byte[] dataA = stream.toByteArray();

        stream = new ByteArrayOutputStream();
        output = new ArrayOutput(new DataOutputStream(stream));
        writer = instance.orderedWriter(output);

        for (DocumentLengthWordCount dlwc : b) {
            writer.process(dlwc);
        }

        writer.close();
        byte[] dataB = stream.toByteArray();

        ArrayInput aInput = new ArrayInput(new DataInputStream(new ByteArrayInputStream(dataA)));
        DocumentLengthWordCount.DocumentLengthOrder.ShreddedReader aReader = new DocumentLengthWordCount.DocumentLengthOrder.ShreddedReader(
                aInput);

        ArrayInput bInput = new ArrayInput(new DataInputStream(new ByteArrayInputStream(dataB)));
        DocumentLengthWordCount.DocumentLengthOrder.ShreddedReader bReader = new DocumentLengthWordCount.DocumentLengthOrder.ShreddedReader(
                bInput);

        ArrayList<DocumentLengthWordCount.DocumentLengthOrder.ShreddedReader> readers = new ArrayList();
        readers.add(aReader);
        readers.add(bReader);
        DocumentLengthWordCount.DocumentLengthOrder.ShreddedCombiner combiner = new DocumentLengthWordCount.DocumentLengthOrder.ShreddedCombiner(
                readers, true);
        DocLenWordCountListener listener = new DocLenWordCountListener();

        combiner.setProcessor(listener);
        combiner.run();

        Comparator<DocumentLengthWordCount> comparator = instance.lessThan();

        for (int i = 0; i < fullList.size(); i++) {
            assertEquals("count: " + i + " expected: " + fullList.get(i) + " actual: " + listener.all.
                    get(i),
                    0, comparator.compare(fullList.get(i), listener.all.get(i)));
        }
    }

    public void testDuplicateElimination() throws IOException {
        DocumentLengthWordCount.DocumentLengthOrder.DuplicateEliminator eliminator = new DocumentLengthWordCount.DocumentLengthOrder.DuplicateEliminator(
                new DocLenWordCountListener());

        for (int i = 0; i < 5; i++) {
            eliminator.processDocument("doca");
            eliminator.processLength(5);
            eliminator.processTuple("aaab", 3);
        }
    }
}
