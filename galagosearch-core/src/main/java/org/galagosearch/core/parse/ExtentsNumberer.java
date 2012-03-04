// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.IOException;
import java.util.HashMap;
import org.galagosearch.core.types.DocumentExtent;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.types.DocumentExtent")
@OutputClass(className = "org.galagosearch.core.types.NumberedExtent")
@Verified
public class ExtentsNumberer extends StandardStep<DocumentExtent, NumberedExtent> {
    HashMap<String, Integer> documentNumbers = new HashMap();

    public void process(DocumentExtent object) throws IOException {
        int documentNumber = documentNumbers.get(object.identifier);
        processor.process(new NumberedExtent(Utility.makeBytes(object.extentName),
                documentNumber, object.begin, object.end));
    }

    public ExtentsNumberer(TupleFlowParameters parameters) throws IOException {
        TypeReader<NumberedDocumentData> reader = parameters.getTypeReader("numberedDocumentData");
        NumberedDocumentData docData;

        while ((docData = reader.read()) != null) {
            documentNumbers.put(docData.identifier, docData.number);
        }
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        Verification.verifyTypeReader("nubmeredDocumentData", DocumentExtent.class, parameters,
                handler);
    }
}
