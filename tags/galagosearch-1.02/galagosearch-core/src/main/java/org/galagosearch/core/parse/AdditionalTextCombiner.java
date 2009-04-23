// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import org.galagosearch.core.types.AdditionalDocumentText;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 * Adds tuples of type AdditionalDocumentText to the end of the text field in
 * a document.  The AdditionalDocumentText stream is specified in the
 * textSource parameter.  This stage should be used before document tokenizing.
 * 
 * @author trevor
 */
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.parse.Document")
public class AdditionalTextCombiner extends StandardStep<Document, Document> {
    TypeReader<AdditionalDocumentText> text;
    AdditionalDocumentText last;

    @SuppressWarnings("unchecked")
    public AdditionalTextCombiner(TupleFlowParameters parameters) throws IOException {
        String readerName = parameters.getXML().get("textSource");
        text = parameters.getTypeReader(readerName);
        last = text.read();
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!Verification.requireParameters(new String[] { "textSource" }, parameters.getXML(), handler))
            return;

        String readerName = parameters.getXML().get("textSource");
        Verification.verifyTypeReader(readerName, AdditionalDocumentText.class, parameters, handler);
    }

    @Override
    public void process(Document document) throws IOException {
        while (last != null && Utility.compare(last.identifier, document.identifier) < 0) {
            last = text.read();
        }

        if (last != null && last.identifier.equals(document.identifier)) {
            document.text += last.text;
        }

        processor.process(document);
    }
}
