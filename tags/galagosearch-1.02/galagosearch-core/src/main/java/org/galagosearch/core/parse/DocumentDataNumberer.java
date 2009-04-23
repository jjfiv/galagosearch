// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import org.galagosearch.core.types.DocumentData;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * <p>Sequentially numbers document data objects.</p>
 *
 * <p>The point of this class is to assign small numbers to each document.  This
 * would be simple if only one process was parsing documents, but in fact there are many
 * of them doing the job at once.  So, we extract DocumentData records from each document,
 * put them into a single list, and assign numbers to them.  These NumberedDocumentData
 * records are then used to assign numbers to index positings.
 * </p>
 * 
 * @author trevor
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.DocumentData")
@OutputClass(className = "org.galagosearch.core.types.NumberedDocumentData")
public class DocumentDataNumberer extends StandardStep<DocumentData, NumberedDocumentData> {
    int number = 0;

    public void process(DocumentData data) throws IOException {
        NumberedDocumentData numbered = new NumberedDocumentData();
        numbered.identifier = data.identifier;
        numbered.url = data.url;
        numbered.textLength = data.textLength;
        numbered.number = number;
        ++number;

        processor.process(numbered);
    }
}
