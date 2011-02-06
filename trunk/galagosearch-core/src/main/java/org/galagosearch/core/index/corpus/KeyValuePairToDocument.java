// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * <p>This is used in conjunction with DocumentToKeyValuePair.  Since Document
 * is not a real Galago type, it needs to be converted to a KeyValuePair in order
 * to be passed between stages (or to a Sorter).</p>
 * 
 * @author trevor
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.KeyValuePair")
@OutputClass(className = "org.galagosearch.core.parse.Document")
public class KeyValuePairToDocument extends StandardStep<KeyValuePair, Document> {

    boolean compressed;

    public KeyValuePairToDocument() {
        compressed = false; // used for testing
    }

    public KeyValuePairToDocument(TupleFlowParameters parameters) {
        compressed = parameters.getXML().get("compressed", true);
    }

    public void process(KeyValuePair object) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(object.value);
        Document document;

        try {
            if (compressed) {
                ObjectInputStream input = new ObjectInputStream(new GZIPInputStream(stream));
                document = (Document) input.readObject();
            } else {
                ObjectInputStream input = new ObjectInputStream(stream);
                document = (Document) input.readObject();
            }
        } catch (ClassNotFoundException ex) {
            System.err.println(ex.toString());
            throw new RuntimeException("Unable to extract document from KeyValuePair.");
        }

        processor.process(document);
    }
}
