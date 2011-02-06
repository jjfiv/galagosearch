// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPOutputStream;
import org.galagosearch.core.parse.Document;

import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Writes documents to a file
 *  - new output file is created in the folder specified by "filename"
 *  - document.identifier -> output-file, byte-offset is passed on
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.types.KeyValuePair")
public class DocumentToKeyValuePair extends StandardStep<Document, KeyValuePair> implements KeyValuePair.Source {

    boolean compressed;

    public DocumentToKeyValuePair() {
        compressed = false; // used for testing
    }

    public DocumentToKeyValuePair(TupleFlowParameters parameters) {
        compressed = parameters.getXML().get("compressed", true);
    }

    public void process(Document document) throws IOException {
        ByteArrayOutputStream array = new ByteArrayOutputStream();
        ObjectOutputStream output;
        if (compressed) {
            output = new ObjectOutputStream(new GZIPOutputStream(array));
        } else {
            output = new ObjectOutputStream(array);
        }

        output.writeObject(document);
        output.close();

        byte[] key = Utility.fromString(document.identifier);
        byte[] value = array.toByteArray();
        KeyValuePair pair = new KeyValuePair(key, value);
        processor.process(pair);

    }
}
