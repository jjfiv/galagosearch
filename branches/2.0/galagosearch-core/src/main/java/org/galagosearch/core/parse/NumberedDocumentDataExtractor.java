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
 * Similar to DocumentDataExtractor:
 * Copies a few pieces of metadata about a document (identifier, url, length) from
 * a document object. Additionally maintains a document number.
 * 
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.parse.NumberedDocument")
@OutputClass(className = "org.galagosearch.core.types.NumberedDocumentData")
@Verified
public class NumberedDocumentDataExtractor extends StandardStep<NumberedDocument, NumberedDocumentData> {
    public void process(NumberedDocument document) throws IOException {
    	NumberedDocumentData data = new NumberedDocumentData();
        data.identifier = document.identifier;
        data.url = "";
        if (document.metadata.containsKey("url")) {
            data.url = document.metadata.get("url");
        }
        data.textLength = document.terms.size();
        data.number = document.number;

        processor.process(data);
    }
}
