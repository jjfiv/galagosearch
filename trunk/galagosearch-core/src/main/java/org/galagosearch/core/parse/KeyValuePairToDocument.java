// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
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
    @Override
    public void process(KeyValuePair object) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(object.value);
        ObjectInputStream input = new ObjectInputStream(stream);
        Document document;
        try {
            document = (Document) input.readObject();
        } catch (ClassNotFoundException ex) {
            IOException e = new IOException("Expected to find a serialized document here, " +
                                            "but found something else instead.");
            e.initCause(ex);
            throw e;
        }

        processor.process(document);
    }
}
