// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index.ngram;

import java.io.IOException;

import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * <p> Numbers documents -- all documents numbered == 1.</p>
 *
 * <p>The point of this class is to create NumberedDocuments for the 
 * NgramProducer. It specifically avoids spending too much time 
 * or memory trying to number documents correctly. It should
 * only be used when the document numbers are not to be used later.
 * </p>
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.parse.NumberedDocument")
public class DummyDocumentNumberer extends StandardStep<Document, NumberedDocument> {
    
    public void process(Document doc) throws IOException {
      NumberedDocument numdoc;
      if(doc instanceof NumberedDocument){
        numdoc = (NumberedDocument) doc;
      } else{
        numdoc = new NumberedDocument();
        numdoc.identifier = doc.identifier;
        numdoc.metadata = doc.metadata;
        numdoc.tags = doc.tags;
        numdoc.terms = doc.terms;
        numdoc.text = doc.text;
        numdoc.fileId = doc.fileId;
      }
      numdoc.number = 1;
      processor.process(numdoc);
    }
}
