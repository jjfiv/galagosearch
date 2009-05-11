// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import org.galagosearch.core.types.DocumentWordPosition;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@Verified
@InputClass(className="org.galagosearch.core.parse.Document")
@OutputClass(className="org.galagosearch.core.types.DocumentWordPosition")
public class PostingsPositionExtractor extends StandardStep<Document, DocumentWordPosition> {
    public void process(Document object) throws IOException {
        for(int i=0; i<object.terms.size(); i++) {
            String term = object.terms.get(i);
            if (term == null)
                continue;
            
            processor.process(new DocumentWordPosition(object.identifier, Utility.makeBytes(term), i));
        }
    }
}
