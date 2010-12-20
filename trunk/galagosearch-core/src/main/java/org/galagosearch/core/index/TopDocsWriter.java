/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index;

import gnu.trove.TIntArrayList;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import org.galagosearch.core.types.TopDocsEntry;
import org.galagosearch.tupleflow.FileSource;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 *
 * @author irmarc
 */
@InputClass(className="org.galagosearch.core.types.TopDocsEntry", order={"+word", "+document"})
public class TopDocsWriter implements TopDocsEntry.WordDocumentOrder.ShreddedProcessor {

    public class TopDocsList implements IndexElement {
        byte[] key;
        CompressedRawByteBuffer data;
        CompressedByteBuffer header;
        long count;
        int lastDoc;

        public TopDocsList(byte[] key) {
            this.key = key;
            data = new CompressedRawByteBuffer();
            header = new CompressedByteBuffer();
            lastDoc = 0;
            count = 0;
        }

        public byte[] key() {
            return key;
        }

        public void addDocument(int document) {
            data.add(document-lastDoc);
            lastDoc = document;
        }

        // Write the score to the buffer, the # of docs,  then the doc ids
        public void addExpandedScore(int freq, int length) {
            data.add(freq);
            data.add(length);
            count++;
        }

        public long dataLength() {
            long length = 0;
            length += header.length();
            length += data.length();
            return length;
        }

        public void write(OutputStream stream) throws IOException {
            header.write(stream);
            data.write(stream);
        }

        public void close() {
            header.add(count);
        }
    }

    IndexWriter writer;
    TopDocsList currentList;

    public TopDocsWriter(TupleFlowParameters parameters) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(parameters.getXML().get("directory"));
        sb.append(File.separator);
        sb.append(parameters.getXML().get("part"));
        sb.append(".topdocs");
        parameters.getXML().set("filename", sb.toString());
        writer = new IndexWriter(parameters);
        writer.getManifest().add("writerClass", getClass().getName());
        writer.getManifest().add("readerClass", TopDocsReader.class.getName());
        currentList = null;
    }

     public void processWord(byte[] word) throws IOException {
        if (currentList != null) {
            currentList.close();
            writer.add(currentList);
        }
        currentList = new TopDocsList(word);
    }

    public void processDocument(int document) throws IOException {
        currentList.addDocument(document);
    }

    public void processTuple(double probability, int count, int doclength) throws IOException {
        currentList.addExpandedScore(count, doclength);
    }

    public void close() throws IOException {
        writer.close();
    }

    public void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        FileSource.verify(parameters, handler);
    }

}   


