// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.parse.Document")
@Verified
public class FieldConflater extends StandardStep<Document, Document> {
    HashMap<String, String> conflations = new HashMap<String, String>();

    public FieldConflater(TupleFlowParameters parameters) {
        List<Value> values = parameters.getXML().list("field");

        for (Value field : values) {
            List<String> sources = field.stringList("source");
            String destination = field.get("destination");

            for (String s : sources) {
                conflations.put(s, destination);
            }
        }
    }

    public void process(Document document) throws IOException {
        for (Tag tag : document.tags) {
            if (conflations.containsKey(tag.name)) {
                tag.name = conflations.get(tag.name);
            }
        }

        processor.process(document);
    }
}
