// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.mergeindex.extractor;

import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.DocumentSplit")
@OutputClass(className = "org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator")
public class ExtractLengths extends StandardStep<DocumentSplit, NumberedDocumentDataIterator> {
  ArrayList<StructuredIndex> s = new ArrayList();

  public void process(DocumentSplit file) throws IOException {
    StructuredIndex i = new StructuredIndex(file.fileName);
    NumberedDocumentDataIterator nddi = i.getDocumentLengthsIterator();
    nddi.indexId = file.fileId;
    processor.process(nddi);

    s.add(i);
  }

  public void close() throws IOException{
    processor.close();
    for(StructuredIndex i : s)
      i.close();
  }
}
