// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.core.types.NumberedExtent;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.types.NumberedExtent", order = {"+extentName", "+number", "+begin"})
@Verified
public class ExtentIndexWriter implements NumberedExtent.ExtentNameNumberBeginOrder.ShreddedProcessor {
    long minimumSkipListLength = 2048;
    int skipByteLength = 128;
    byte[] lastWord;
    long lastPosition = 0;
    long lastDocument = 0;
    ExtentListBuffer invertedList;
    OutputStream output;
    long filePosition;
    IndexWriter writer;
    long documentCount = 0;
    long collectionLength = 0;
    Parameters header;

    /**
     * Creates a new instance of ExtentIndexWriter
     */
    public ExtentIndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
        writer = new IndexWriter(parameters);
        writer.getManifest().add("readerClass", ExtentIndexReader.class.getName());
        writer.getManifest().add("writerClass", getClass().toString());
        header = parameters.getXML();
    }

    public void processExtentName(byte[] wordBytes) throws IOException {
        if (invertedList != null) {
            invertedList.close();
            writer.add(invertedList);
            invertedList = null;
        }

        invertedList = new ExtentListBuffer();
        invertedList.setWord(wordBytes);

        assert lastWord == null || 0 != Utility.compare(lastWord, wordBytes) : "Duplicate word";
        lastWord = wordBytes;
    }

    public void processNumber(long document) {
        invertedList.addDocument(document);
    }

    public void processBegin(int begin) throws IOException {
        invertedList.addBegin(begin);
    }

    public void processTuple(int end) throws IOException {
        invertedList.addEnd(end);
    }

    public void close() throws IOException {
        if (invertedList != null) {
            invertedList.close();
            writer.add(invertedList);
        }

        writer.close();
    }
}
