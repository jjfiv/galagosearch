// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;
import java.io.IOException;
import org.galagosearch.core.types.DocumentExtent;

/**
 * Converts all tags from a document object into DocumentExtent tuples.
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.types.DocumentExtent")
@Verified
public class ExtentExtractor extends StandardStep<Document, DocumentExtent> {
    public void process(Document document) throws IOException {
        for (Tag tag : document.tags) {
            processor.process(new DocumentExtent(tag.name,
                    document.identifier,
                    tag.begin,
                    tag.end));
        }
    }
}
