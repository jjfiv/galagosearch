/*
 *  BSD License (http://www.galagosearch.org/license)
 */

package org.galagosearch.core.index.merge;

import java.io.IOException;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@OutputClass( className="org.galagosearch.core.types.DocumentSplit", order={"+fileId"} )
public class IndexNumberer implements ExNihiloSource<DocumentSplit> {
  public Processor<DocumentSplit> processor;
  TupleFlowParameters parameters;
  
  public IndexNumberer(TupleFlowParameters parameters){
    this.parameters = parameters;
  }

  public void run() throws IOException {
    int i = 0 ;
    int total = parameters.getXML().stringList("inputPath").size();
    for(String inputIndex : parameters.getXML().stringList("inputPath")){
      processor.process(new DocumentSplit(inputIndex,"",false,new byte[0],new byte[0],i,total));
      i++;
    }

    processor.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }
}
