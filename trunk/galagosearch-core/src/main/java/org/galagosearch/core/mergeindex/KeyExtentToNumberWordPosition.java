package org.galagosearch.core.mergeindex;

import java.io.IOException;
import java.util.Comparator;

import org.galagosearch.core.types.NumberWordPosition;
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
@OutputClass(className = "org.galagosearch.core.types.NumberWordPosition")
public class KeyExtentToNumberWordPosition extends StandardStep<KeyExtent, NumberWordPosition> {

  public KeyExtentToNumberWordPosition(TupleFlowParameters p){
    // Nothing needs to be done
  }
  public KeyExtentToNumberWordPosition(Processor<NumberWordPosition> p) throws IncompatibleProcessorException {
    this.setProcessor(p);
  }

  public void process(KeyExtent ke) throws IOException {
    NumberWordPosition nwp = new NumberWordPosition(ke.document, Utility.makeBytes(ke.key), ke.extent.begin);
    processor.process(nwp);
  }
}
