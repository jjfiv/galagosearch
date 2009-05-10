// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 * Writes the document lengths file based on data in NumberedDocumentData tuples.
 * The document lengths data is used by StructuredIndex because it's a key
 * input to more scoring functions.
 * 
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.types.NumberedDocumentData", order = {"+number"})
public class DocumentLengthsWriter implements Processor<NumberedDocumentData> {
    DataOutputStream output;
    int document = 0;
    Counter documentsWritten = null;

    /** Creates a new instance of DocumentLengthsWriter */
    public DocumentLengthsWriter(TupleFlowParameters parameters) throws FileNotFoundException {
        String filename = parameters.getXML().get("filename");
        Utility.makeParentDirectories(filename);
        output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
        documentsWritten = parameters.getCounter("Documents Written");
    }

    public void close() throws IOException {
        output.close();
    }

    public void process(NumberedDocumentData object) throws IOException {
        assert document <= object.number : "d: " + document + " o.d:" + object.number;

        while (document < object.number) {
            output.writeInt(0);
            document++;
        }

        output.writeInt(object.textLength);
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
