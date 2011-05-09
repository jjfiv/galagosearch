// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.mergeindex.parallel;

import java.io.IOException;

import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

@Verified
@InputClass(className="org.galagosearch.core.types.DocumentSplit")
@OutputClass(className="org.galagosearch.core.mergeindex.parallel.MappedDocument")
public class CreateNumberMapping extends StandardStep<DocumentSplit, MappedDocument>{
  int newDocumentNumber = 0;
  
  public void process(DocumentSplit index) throws IOException {
    StructuredIndex i = new StructuredIndex(index.fileName);
    NumberedDocumentDataIterator nddi = i.getDocumentNamesIterator();

    do{
      NumberedDocumentData ndd = nddi.getDocumentData();
      processor.process(new MappedDocument(newDocumentNumber, index.fileId, ndd.number));
      newDocumentNumber++;

    } while(nddi.nextRecord());

    i.close();
  }
}
