package org.galagosearch.core.mergeindex.extractor;

import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.DocumentSplit")
@OutputClass(className = "org.galagosearch.tupleflow.Parameters")
public class ExtractManifest extends StandardStep<DocumentSplit, Parameters> {
  ArrayList<StructuredIndex> s = new ArrayList();

  public void process(DocumentSplit file) throws IOException {
    StructuredIndex i = new StructuredIndex(file.fileName);
    processor.process(i.getManifest());

    s.add(i);
  }

  public void close() throws IOException{
    processor.close();
    for(StructuredIndex i : s)
      i.close();
  }
}
