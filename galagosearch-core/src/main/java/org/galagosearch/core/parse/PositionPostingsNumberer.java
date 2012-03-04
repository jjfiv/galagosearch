// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;
import org.galagosearch.core.types.DocumentWordPosition;
import org.galagosearch.core.types.NumberWordPosition;
import java.io.IOException;
import java.util.HashMap;
import org.galagosearch.core.types.NumberedDocumentData;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.types.DocumentWordPosition")
@OutputClass(className = "org.galagosearch.core.types.NumberWordPosition")
public class PositionPostingsNumberer extends StandardStep<DocumentWordPosition, NumberWordPosition>
        implements DocumentWordPosition.Processor, NumberWordPosition.Source {
    HashMap<String, Integer> documentNumbers = new HashMap();

    public void process(DocumentWordPosition object) throws IOException {
        assert documentNumbers.get(object.document) != null : "" + object.document +
                " has no name, even with " + documentNumbers.size() + " doc names.";
        processor.process(
                new NumberWordPosition(documentNumbers.get(object.document),
                                       object.word,
                                       object.position));
    }

    public PositionPostingsNumberer(TupleFlowParameters parameters) throws IOException {
        TypeReader<NumberedDocumentData> reader = parameters.getTypeReader("numberedDocumentData");
        NumberedDocumentData ndd;

        while ((ndd = reader.read()) != null) {
            documentNumbers.put(ndd.identifier, ndd.number);
        }
    }

    public Class<DocumentWordPosition> getInputClass() {
        return DocumentWordPosition.class;
    }

    public Class<NumberWordPosition> getOutputClass() {
        return NumberWordPosition.class;
    }

    public static String getInputClass(TupleFlowParameters parameters) {
        return DocumentWordPosition.class.getName();
    }

    public static String getOutputClass(TupleFlowParameters parameters) {
        return NumberWordPosition.class.getName();
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        Verification.verifyTypeReader("numberedDocumentData", NumberedDocumentData.class,
                parameters, handler);
    }
}
