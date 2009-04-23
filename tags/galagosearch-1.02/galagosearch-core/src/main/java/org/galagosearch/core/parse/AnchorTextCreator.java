// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import org.galagosearch.core.types.AdditionalDocumentText;
import org.galagosearch.core.types.ExtractedLink;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.DocumentLinkData")
@OutputClass(className = "org.galagosearch.core.types.AdditionalDocumentText")
public class AnchorTextCreator extends StandardStep<DocumentLinkData, AdditionalDocumentText> {
    @Override
    public void process(DocumentLinkData object) throws IOException {
        AdditionalDocumentText additional = new AdditionalDocumentText();
        StringBuilder extraText = new StringBuilder();

        additional.identifier = object.identifier;
        for (ExtractedLink link : object.links) {
            extraText.append("<anchor>");
            extraText.append(link.anchorText);
            extraText.append("</anchor>");
        }
        additional.text = extraText.toString();

        processor.process(additional);
    }
}
