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
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.NumberedExtent")
@OutputClass(className = "org.galagosearch.core.types.NumberedExtent", order = {"+extentName", "+number", "+begin"})
public class AssertNumberedExtentOrder extends StandardStep<NumberedExtent, NumberedExtent> implements NumberedExtent.Source{
  NumberedExtent last;
  Comparator<NumberedExtent> c = new NumberedExtent.ExtentNameNumberBeginOrder().lessThan();

  public AssertNumberedExtentOrder(TupleFlowParameters p){
    // nothing
  }
  public AssertNumberedExtentOrder(Processor<NumberedExtent> p) throws IncompatibleProcessorException{
    this.setProcessor(p);
  }
  
  public void process(NumberedExtent cur) throws IOException {
    if(last == null) last = cur;
    assert c.compare(last, cur) <= 0;
    last = cur;
    processor.process(cur);
  }

}
