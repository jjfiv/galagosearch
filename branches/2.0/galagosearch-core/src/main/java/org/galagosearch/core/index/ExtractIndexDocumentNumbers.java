// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.IOException;

import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.NumberedDocument;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * <p> Numbers documents using an existing index.</p>
 *
 * <p>The point of this class is to find the small number 
 * that is already associated with each document.  
 * NumberedDocuments are generated.
 * </p>
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.parse.NumberedDocument")
public class ExtractIndexDocumentNumbers extends StandardStep<Document, NumberedDocument> {
    StructuredIndex index;
    int i = 0;
    
    public ExtractIndexDocumentNumbers(TupleFlowParameters parameters) throws IOException{
      String indexPath = parameters.getXML().get("indexPath");
      index = new StructuredIndex(indexPath);
    }
    
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
      }
      try{
        numdoc.number = index.getIdentifier(doc.identifier);
      } catch (Exception e){
        //System.err.println("can not find name: " + doc.identifier);
        throw new IOException("Can not find document number for document: " + doc.identifier);
      }
      processor.process(numdoc);
    }
}
