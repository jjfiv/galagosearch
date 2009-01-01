// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * <p>This is used in conjunction with KeyValuePairToDocument.  Since Document
 * is not a real Galago type, it needs to be converted to a KeyValuePair in order
 * to be passed between stages (or to a Sorter).</p>

 * @author trevor
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.types.KeyValuePair")
public class DocumentToKeyValuePair extends StandardStep<Document, KeyValuePair> {
    @Override
    public void process(Document document) throws IOException {
        ByteArrayOutputStream array = new ByteArrayOutputStream();
        ObjectOutputStream output = new ObjectOutputStream(array);
        output.writeObject(document);
        output.close();

        byte[] key = Utility.makeBytes(document.identifier);
        byte[] value = array.toByteArray();
        KeyValuePair pair = new KeyValuePair(key, value);
        processor.process(pair);
    }
}
