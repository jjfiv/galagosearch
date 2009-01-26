// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.galagosearch.core.types.NumberWordProbability;
import org.galagosearch.tupleflow.TupleFlowParameters;

/**
 *
 * @author trevor
 */
public class SparseFloatListWriter implements 
        NumberWordProbability.NumberWordOrder.ShreddedProcessor {
    IndexWriter writer;
    DoubleInvertedList list;

    public class DoubleInvertedList implements IndexElement {
        BackedCompressedByteBuffer data = new BackedCompressedByteBuffer();
        CompressedByteBuffer header = new CompressedByteBuffer();
        int lastDocument;
        int documentCount;
        byte[] word;

        public DoubleInvertedList(byte[] word) {
            this.word = word;
            this.lastDocument = 0;
            this.documentCount = 0;
        }

        public void write(final OutputStream stream) throws IOException {
            header.write(stream);
            data.write(stream);
        }

        public void addDocument(int document) throws IOException {
            data.add(document - lastDocument);
            documentCount++;
            lastDocument = document;
        }

        public void addProbability(double probability) throws IOException {
            data.addFloat((float) probability);
        }

        public byte[] key() {
            return word;
        }

        public long dataLength() {
            return data.length() + header.length();
        }

        public void close() {
            header.add(documentCount);
        }
    }

    /** Creates a new instance of DoubleListWriter */
    public SparseFloatListWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
        writer = new IndexWriter(parameters);
        writer.getManifest().add("readerClass", SparseFloatListReader.class.getName());
        writer.getManifest().add("writerClass", getClass().getName());
    }

    public void processWord(byte[] word) throws IOException {
        if (list != null) {
            list.close();
            writer.add(list);
        }

        list = new DoubleInvertedList(word);
    }

    public void processNumber(int number) throws IOException {
        list.addDocument(number);
    }

    public void processTuple(double probability) throws IOException {
        list.addProbability(probability);
    }

    public void close() throws IOException {
        if (list != null) {
            list.close();
            writer.add(list);
        }

        writer.close();
    }
}
