// BSD License (http://www.galagosearch.org/license)
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
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.NumberWordPosition")
@OutputClass(className = "org.galagosearch.core.types.NumberWordPosition", order = {"+word", "+document", "+position"})
public class AssertNumberWordPositionOrder extends StandardStep<NumberWordPosition, NumberWordPosition> implements NumberWordPosition.Source{
  NumberWordPosition last;
  Comparator<NumberWordPosition> c = new NumberWordPosition.WordDocumentPositionOrder().lessThan();

  public AssertNumberWordPositionOrder(TupleFlowParameters p){
    // nothing
  }
  public AssertNumberWordPositionOrder(Processor<NumberWordPosition> p) throws IncompatibleProcessorException{
    this.setProcessor(p);
  }

  
  public void process(NumberWordPosition cur) throws IOException {
    if(last == null) last = cur;
    assert c.compare(last, cur) <= 0;
    last = cur;
    processor.process(cur);
  }

}
