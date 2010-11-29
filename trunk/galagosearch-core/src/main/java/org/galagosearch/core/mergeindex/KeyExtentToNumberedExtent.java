package org.galagosearch.core.mergeindex;

import java.io.IOException;
import java.util.Comparator;

import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

@Verified
@InputClass(className = "org.galagosearch.core.mergeindex.KeyExtent")
@OutputClass(className = "org.galagosearch.core.types.NumberedExtent")
public class KeyExtentToNumberedExtent extends StandardStep<KeyExtent, NumberedExtent> {

  public KeyExtentToNumberedExtent(TupleFlowParameters p){
    // Nothing needs to be done
  }

  public KeyExtentToNumberedExtent(Processor<NumberedExtent> p) throws IncompatibleProcessorException {
    this.setProcessor(p);
  }

  public void process(KeyExtent ke) throws IOException {
    NumberedExtent ne = new NumberedExtent(Utility.fromString(ke.key), ke.document, ke.extent.begin, ke.extent.end);
    processor.process(ne);
  }
}
