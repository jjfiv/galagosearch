// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index;

import java.io.File;
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
    private final DocumentNameReader.KeyIterator namesIterator;
    
    public ExtractIndexDocumentNumbers(TupleFlowParameters parameters) throws IOException{
      String namesPath = parameters.getXML().get("indexPath") + File.separator + "names.reverse";
      namesIterator = ((DocumentNameReader) StructuredIndex.openIndexPart(namesPath)).getIterator();
    }
    
    public void process(Document doc) throws IOException {
      NumberedDocument numdoc;
      if(doc instanceof NumberedDocument){
        numdoc = (NumberedDocument) doc;
      } else{
        numdoc = new NumberedDocument(doc);
      }
      try{
        namesIterator.findKey( numdoc.identifier );
        numdoc.number = namesIterator.getCurrentIdentifier();
      } catch (Exception e){
        throw new IOException("Can not find document number for document: " + doc.identifier);
      }
      processor.process(numdoc);
    }
}
