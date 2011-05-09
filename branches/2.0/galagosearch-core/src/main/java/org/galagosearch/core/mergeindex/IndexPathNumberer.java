// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.mergeindex;

import java.io.IOException;
import java.util.List;

import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Parameters.Value;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * 
 *  
 *  
 * @author sjh
 */
@Verified
@OutputClass(className = "org.galagosearch.core.types.DocumentSplit", order={"+fileId"})
public class IndexPathNumberer implements ExNihiloSource<DocumentSplit> {
  public Processor<DocumentSplit> processor;
  TupleFlowParameters parameters;
  StructuredIndex index;
  int fileId = 0;
  int totalIndexes = 0;

  public IndexPathNumberer(TupleFlowParameters parameters) throws IOException{
    this.parameters = parameters;
  }

  private void processIndex(String indexPath) throws IOException{

    DocumentSplit split = new DocumentSplit(indexPath, "index", false, new byte[0], new byte[0], fileId, totalIndexes);
    processor.process( split );
    fileId++;

  }

  public void run() throws IOException {
    if (parameters.getXML().containsKey("inputIndex")) {
      List<Value> indexes = parameters.getXML().list("inputIndex");
      totalIndexes = indexes.size();

      for (Value index : indexes) {
        String indexPath = index.toString();
        processIndex(indexPath);
      }
    } else {
      System.err.println("Missing inputIndex paramaters");
    }

    processor.close();
  }


  public void setProcessor(Step processor)
    throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }
}
