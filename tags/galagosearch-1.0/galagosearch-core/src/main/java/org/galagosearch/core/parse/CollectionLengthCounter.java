// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import org.galagosearch.core.types.DocumentData;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.tupleflow.types.XMLFragment;

/**
 *
 * @author trevor
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.DocumentData")
@OutputClass(className = "org.galagosearch.tupleflow.types.XMLFragment")
public class CollectionLengthCounter extends StandardStep<DocumentData, XMLFragment> {
    long collectionLength = 0;
    long documentCount = 0;

    public void process(DocumentData data) {
        collectionLength += data.textLength;
        documentCount += 1;
    }

    @Override
    public void close() throws IOException {
        processor.process(new XMLFragment("collectionLength", Long.toString(collectionLength)));
        processor.process(new XMLFragment("documentCount", Long.toString(documentCount)));
        super.close();
    }
}
