// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;
import java.io.IOException;
import org.galagosearch.core.types.DocumentExtent;
import org.galagosearch.core.types.NumberedExtent;

/**
 * Converts all tags from a document object into NumberedExtent tuples.
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.parse.NumberedDocument")
@OutputClass(className = "org.galagosearch.core.types.NumberedExtent")
@Verified
public class NumberedExtentExtractor extends StandardStep<NumberedDocument, NumberedExtent> {
	public void process(NumberedDocument document) throws IOException {
        for (Tag tag : document.tags) {
            processor.process(new NumberedExtent(Utility.fromString(tag.name),
            		document.number,
            		tag.begin,
                    tag.end));
        }
	}
}
