// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import org.galagosearch.core.types.DocumentWordPosition;
import org.galagosearch.core.types.NumberWordPosition;
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
@InputClass(className="org.galagosearch.core.parse.NumberedDocument")
@OutputClass(className="org.galagosearch.core.types.NumberWordPosition")
public class NumberedPostingsPositionExtractor extends StandardStep<NumberedDocument, NumberWordPosition> {
    public void process(NumberedDocument object) throws IOException {
        for(int i=0; i<object.terms.size(); i++) {
            String term = object.terms.get(i);
            if (term == null)
                continue;
            
            processor.process(new NumberWordPosition(object.number, Utility.fromString(term), i));
        }
    }
}
