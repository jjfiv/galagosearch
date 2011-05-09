// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.IOException;
import org.galagosearch.core.types.DocumentData;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * <p>Sequentially numbers document data objects.</p>
 *
 * <p>The point of this class is to assign small numbers to each document.  This
 * would be simple if only one process was parsing documents, but in fact there are many
 * of them doing the job at once.  So, we extract DocumentData records from each document,
 * put them into a single list, and assign numbers to them.  These NumberedDocumentData
 * records are then used to assign numbers to index postings.
 * </p>
 * 
 * @author trevor
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.parse.NumberedDocument")
public class FastDocumentNumberer extends StandardStep<Document, NumberedDocument> {
  int fileId = -1;
  int curNum = -1;
  int increment = -1;
  
  
    public void process(Document doc) throws IOException {
      if(fileId != doc.fileId){
        fileId = doc.fileId;
        increment = doc.totalFileCount;
        curNum = doc.fileId;
      }
      
      NumberedDocument numDoc = new NumberedDocument(doc);
      numDoc.number = curNum;
      
      curNum += increment;
      
      processor.process(numDoc);
    }
}
