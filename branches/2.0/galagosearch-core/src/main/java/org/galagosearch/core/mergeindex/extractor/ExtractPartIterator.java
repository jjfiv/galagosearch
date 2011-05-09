// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.mergeindex.extractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.retrieval.structured.ExtentIndexIterator;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.DocumentSplit")
@OutputClass(className = "org.galagosearch.core.retrieval.structured.ExtentIndexIterator")
public class ExtractPartIterator extends StandardStep<DocumentSplit, ExtentIndexIterator> {
  String part;

  ArrayList<StructuredIndex> s = new ArrayList();

  public ExtractPartIterator(TupleFlowParameters p){
    part = p.getXML().get("part");
  }

  public void process(DocumentSplit file) throws IOException {
    StructuredIndex i = new StructuredIndex(file.fileName);
    if(i.containsPart(part)){
      ExtentIndexIterator eii = i.getExtentIterator(part);
      if(eii != null){
        eii.indexId = file.fileId;
        processor.process(eii);
      }
    }

    s.add(i);
  }

  public void close() throws IOException{
    processor.close();
    for(StructuredIndex i : s)
      i.close();
  }
}
