// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import org.galagosearch.core.index.GenericElement;
import org.galagosearch.core.index.IndexWriter;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.VByteOutput;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 * Writes document text and metadata to an index file.  The output files
 * are in '.corpus' format, which can be fed to UniversalParser as an input
 * to indexing.  The '.corpus' format is also convenient for quickly
 * finding individual documents.
 * 
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.parse.Document")
public class DocumentIndexWriter implements Processor<Document> {
    IndexWriter writer;
    Counter documentsWritten;
    
    public DocumentIndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
        Parameters p = new Parameters();
        p.add("isCompressed", "true");
        writer = new IndexWriter(parameters.getXML().get("filename"), p);
        documentsWritten = parameters.getCounter("Documents Written");
    }
    
    public void close() throws IOException {
        writer.close();
    }

    public void process(Document document) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        VByteOutput output = new VByteOutput(new DataOutputStream(stream));
        
        output.writeString(document.text);
        for (Map.Entry<String, String> entry : document.metadata.entrySet()) {
            output.writeString(entry.getKey());
            output.writeString(entry.getValue());
        }
        
        writer.add(new GenericElement(document.identifier, stream.toByteArray()));
        if (documentsWritten != null)
            documentsWritten.increment();
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!parameters.getXML().containsKey("filename")) {
            handler.addError("DocumentIndexWriter requires an 'filename' parameter.");
            return;
        }

        String index = parameters.getXML().get("filename");
        Verification.requireWriteableFile(index, handler);
    }
}
