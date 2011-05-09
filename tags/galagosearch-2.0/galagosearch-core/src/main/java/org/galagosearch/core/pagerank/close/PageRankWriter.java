// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.pagerank.close;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.galagosearch.core.types.PREntry;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 * Writes the pagerank values to pagerank file.
 * 
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.types.PREntry", order = {"+docNum"})
public class PageRankWriter implements Processor<PREntry> {
    DataOutputStream output;
    int document = 0;
    int offset = 0;
    Counter documentsWritten = null;

    /** Creates a new instance of DocumentLengthsWriter */
    public PageRankWriter(TupleFlowParameters parameters) throws FileNotFoundException {
        String filename = parameters.getXML().get("filename");
        Utility.makeParentDirectories(filename);
        output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
        documentsWritten = parameters.getCounter("PageRank Values Written");

        offset = -1;
    }

    public void close() throws IOException {
      output.writeInt(offset);
      output.close();
    }

    public void process(PREntry entry) throws IOException {
        if(offset < 0) offset = (int) entry.docNum;
        assert (document + offset) <= entry.docNum : "d: " + document + " o.d:" + entry.docNum;

        while ((document + offset) < entry.docNum) {
            output.writeDouble(0);
            document++;
        }

        output.writeDouble(entry.score);
        document++;
        if (documentsWritten != null) documentsWritten.increment();
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!parameters.getXML().containsKey("filename")) {
            handler.addError("DocumentLengthsWriter requires an 'filename' parameter.");
            return;
        }

        String filename = parameters.getXML().get("filename");
        Verification.requireWriteableFile(filename, handler);
    }
}
