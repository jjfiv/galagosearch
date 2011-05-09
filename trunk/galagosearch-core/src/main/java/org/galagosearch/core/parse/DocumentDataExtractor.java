// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import org.galagosearch.core.types.DocumentData;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Copies a few pieces of metadata about a document (identifier, url, length) from
 * a document object and stores them in a DocumentData tuple.
 * 
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.types.DocumentData")
@Verified
public class DocumentDataExtractor extends StandardStep<Document, DocumentData> {
    public void process(Document document) throws IOException {
        DocumentData data = new DocumentData();
        data.identifier = document.identifier;
        data.url = "";
        if (document.metadata.containsKey("url")) {
            data.url = document.metadata.get("url");
        }
        data.textLength = document.terms.size();

        processor.process(data);
    }
}
