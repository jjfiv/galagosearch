// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import java.util.HashSet;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;

/**
 *
 * @author trevor
 */

@InputClass(className="org.galagosearch.core.parse.Document")
@OutputClass(className="org.galagosearch.core.parse.Document")
public class DocumentFilter extends StandardStep<Document, Document> {
    HashSet<String> docnos = new HashSet();
    
    /** Creates a new instance of DocumentFilter */
    public DocumentFilter(TupleFlowParameters parameters) {
        Parameters p = parameters.getXML();
        
        for(String docno : p.stringList("identifier")) {
            docnos.add(docno);
        }
    }
    
    public void process(Document document) throws IOException {
        if (docnos.contains(document.identifier))
            processor.process(document);
    }
    
    public Class<Document> getOutputClass() {
        return Document.class;
    }
    
    public Class<Document> getInputClass() {
        return Document.class;
    }
}
