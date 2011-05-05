// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.galagosearch.core.index.corpus.SplitIndexValueWriter;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Source;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.types.NumberedExtent", order = {"+extentName", "+number", "+begin"})
@OutputClass(className = "org.galagosearch.core.types.KeyValuePair", order = {"+key"})
public class ExtentIndexWriter implements NumberedExtent.ExtentNameNumberBeginOrder.ShreddedProcessor,
        Source<KeyValuePair> // parallel index data output
{

    long minimumSkipListLength = 2048;
    int skipByteLength = 128;
    byte[] lastWord;
    long lastPosition = 0;
    long lastDocument = 0;
    GenericIndexWriter writer;
    ExtentListBuffer invertedList;
    OutputStream output;
    long filePosition;
    long documentCount = 0;
    long collectionLength = 0;
    Parameters header;
    boolean parallel;

    /**
     * Creates a new instance of ExtentIndexWriter
     */
    public ExtentIndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
        parameters.getXML().add("readerClass", ExtentIndexReader.class.getName());
        parameters.getXML().add("writerClass", getClass().toString());

        writer = new IndexWriter(parameters);
        header = parameters.getXML();
    }

    public void processExtentName(byte[] wordBytes) throws IOException {
        if (invertedList != null) {
            invertedList.close();
            writer.add(invertedList);
            invertedList = null;
        }

        invertedList = new ExtentListBuffer(header);
        invertedList.setWord(wordBytes);

        assert lastWord == null || 0 != Utility.compare(lastWord, wordBytes) : "Duplicate word";
        lastWord = wordBytes;
    }

    public void processNumber(long document) throws IOException {
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

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!parameters.getXML().containsKey("filename")) {
            handler.addError("ExtentIndexWriter requires a 'filename' parameter.");
            return;
        }

        String index = parameters.getXML().get("filename");
        Verification.requireWriteableFile(index, handler);
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        writer.setProcessor(processor);
    }
}
