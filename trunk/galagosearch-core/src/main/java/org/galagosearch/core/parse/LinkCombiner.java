// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;
import org.galagosearch.core.types.ExtractedLink;
import org.galagosearch.core.types.IdentifiedLink;
import java.io.IOException;
import org.galagosearch.core.types.DocumentData;

/**
 *
 * @author trevor
 */
@OutputClass(className = "org.galagosearch.core.parse.DocumentLinkData")
public class LinkCombiner implements ExNihiloSource<IdentifiedLink>, IdentifiedLink.Source {
    TypeReader<ExtractedLink> extractedLinks;
    TypeReader<DocumentData> documentDatas;
    DocumentLinkData linkData;
    public Processor<DocumentLinkData> processor;

    @SuppressWarnings("unchecked")
    public LinkCombiner(TupleFlowParameters parameters) throws IOException {
        String extractedLinksName = parameters.getXML().get("extractedLinks");
        String documentDatasName = parameters.getXML().get("documentDatas");

        extractedLinks = parameters.getTypeReader(extractedLinksName);
        documentDatas = parameters.getTypeReader(documentDatasName);
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }

    void match(DocumentData docData, ExtractedLink link) {
        if (linkData == null) {
            linkData = new DocumentLinkData();
            linkData.identifier = docData.identifier;
            linkData.url = docData.url;
            linkData.textLength = docData.textLength;
        }
        
        linkData.links.add(link);
    }
    
    void flush() throws IOException {
        if (linkData != null) {
            processor.process(linkData);
        }
    }
    
    public void run() throws IOException {
        ExtractedLink link = extractedLinks.read();
        DocumentData docData = documentDatas.read();

        while (docData != null && link != null) {
            int result = link.destUrl.compareTo(docData.url);
            if (result == 0) {
                match(docData, link);
                link = extractedLinks.read();
            } else {
                if (result < 0) {
                    link = extractedLinks.read();
                } else {
                    docData = documentDatas.read();
                }
            }
        }

        processor.close();
    }

    public Class<IdentifiedLink> getOutputClass() {
        return IdentifiedLink.class;
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!Verification.requireParameters(new String[] { "extractedLinks", "documentDatas" },
                                            parameters.getXML(), handler)) {
            return;
        }

        String extractedLinksName = parameters.getXML().get("extractedLinks");
        String documentDatasName = parameters.getXML().get("documentDatas");

        Verification.verifyTypeReader(extractedLinksName, ExtractedLink.class, parameters, handler);
        Verification.verifyTypeReader(documentDatasName, DocumentData.class, parameters, handler);
    }
}
