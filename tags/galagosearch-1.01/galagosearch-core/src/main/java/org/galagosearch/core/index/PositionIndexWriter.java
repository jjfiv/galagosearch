// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.TreeMap;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.types.NumberWordPosition", order = {"+word", "+document", "+position"})
public class PositionIndexWriter implements
        NumberWordPosition.WordDocumentPositionOrder.ShreddedProcessor {
    int blockSize = 32768;
    byte[] lastWord;
    long lastPosition = 0;
    long lastDocument = 0;
    int skipMinimumBinLength;
    TreeMap<Integer, Integer> skipLengths;

    public class PositionsList implements IndexElement {
        public PositionsList() {
            documents = new BackedCompressedByteBuffer();
            counts = new BackedCompressedByteBuffer();
            positions = new BackedCompressedByteBuffer();
            header = new BackedCompressedByteBuffer();
        }

        public void close() throws IOException {
            int options = 0;

            if (documents.length() > 0) {
                counts.add(positionCount);
            }
            header.add(options);

            header.add(documentCount);
            header.add(totalPositionCount);

            header.add(documents.length());
            header.add(counts.length());
            header.add(positions.length());
        }

        public long dataLength() {
            long listLength = 0;

            listLength += header.length();
            listLength += counts.length();
            listLength += positions.length();
            listLength += documents.length();

            return listLength;
        }

        public void write(final OutputStream output) throws IOException {
            header.write(output);
            header.clear();

            documents.write(output);
            documents.clear();

            counts.write(output);
            counts.clear();

            positions.write(output);
            positions.clear();
        }

        public byte[] key() {
            return word;
        }

        public void setWord(byte[] word) {
            this.word = word;
            this.lastDocument = 0;
            this.lastPosition = 0;
            this.totalPositionCount = 0;
            this.positionCount = 0;
        }

        public void addDocument(long documentID) throws IOException {
            // add the last document's counts
            if (documents.length() > 0) {
                counts.add(positionCount);
            }
            documents.add(documentID - lastDocument);
            lastDocument = documentID;

            lastPosition = 0;
            positionCount = 0;
            documentCount++;
        }

        public void addPosition(int position) throws IOException {
            positionCount++;
            totalPositionCount++;
            positions.add(position - lastPosition);
            lastPosition = position;
        }
        private long lastDocument;
        private int lastPosition;
        private int positionCount;
        private int documentCount;
        private int totalPositionCount;
        public byte[] word;
        public BackedCompressedByteBuffer header;
        public BackedCompressedByteBuffer documents;
        public BackedCompressedByteBuffer counts;
        public BackedCompressedByteBuffer positions;
    }
    long maximumDocumentCount = 0;
    long maximumDocumentNumber = 0;
    PositionsList invertedList;
    DataOutputStream output;
    long filePosition;
    IndexWriter writer;
    long documentCount = 0;
    long collectionLength = 0;

    /**
     * Creates a new instance of BinnedListWriter
     */
    public PositionIndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
        writer = new IndexWriter(parameters);
        writer.getManifest().add("writerClass", getClass().getName());
        writer.getManifest().add("readerClass", PositionIndexReader.class.getName());
    }

    public void processWord(byte[] wordBytes) throws IOException {
        if (invertedList != null) {
            invertedList.close();
            writer.add(invertedList);
            invertedList = null;
        }

        resetDocumentCount();

        invertedList = new PositionsList();
        invertedList.setWord(wordBytes);

        assert lastWord == null || 0 != Utility.compare(lastWord, wordBytes) : "Duplicate word";
        lastWord = wordBytes;
    }

    public void processDocument(int document) throws IOException {
        invertedList.addDocument(document);
        documentCount++;
        maximumDocumentNumber = Math.max(document, maximumDocumentNumber);
        lastDocument = document;
    }

    public void processPosition(int position) throws IOException {
        invertedList.addPosition(position);
    }

    public void processTuple() {
        // does nothing
    }

    private void resetDocumentCount() {
        maximumDocumentCount = Math.max(documentCount, maximumDocumentCount);
        documentCount = 0;
    }

    public void close() throws IOException {
        if (invertedList != null) {
            invertedList.close();
            writer.add(invertedList);
        }

        writer.close();
    }

    public long documentCount() {
        return maximumDocumentNumber;
    }

    public long maximumDocumentCount() {
        return maximumDocumentCount;
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!parameters.getXML().containsKey("filename")) {
            handler.addError("PositionsListWriter requires an 'filename' parameter.");
            return;
        }

        String index = parameters.getXML().get("filename");
        Verification.requireWriteableFile(index, handler);
    }
}

