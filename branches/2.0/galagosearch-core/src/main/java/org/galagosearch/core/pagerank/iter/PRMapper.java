// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.iter;

import java.io.IOException;

import org.galagosearch.core.types.NumberedLink;
import org.galagosearch.core.types.PREntry;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 * distributes pagerank value from document to all outgoing links
 * 
 * @author schiu, sjh
 *
 */
@InputClass(className = "org.galagosearch.core.pagerank.iter.PRDoc")
@OutputClass(className = "org.galagosearch.core.types.PREntry", order = {"+docNum"})
public class PRMapper extends StandardStep<PRDoc, PREntry> {

  private double lambda;

  public PRMapper(TupleFlowParameters parameters) {
    lambda = Double.parseDouble(parameters.getXML().get("lambda"));
  }

  public void process(PRDoc doc) throws IOException {
    // this document has now given away all of it's score
    processor.process(new PREntry(doc.entry.docNum, 0));

    // if it has some links - emit a partial score for each
    if (doc.size() > 0) {
      double contribPerLink = (1.0 - lambda) * doc.entry.score / ((double) doc.size());
      for (NumberedLink l : doc) {
        processor.process(new PREntry(l.destination, contribPerLink));
      }
    }
  }


  /**
   * Makes sure that the TupleFlow parameter file passes a parameter called
   * 'lambda' to this class.
   */
  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("lambda")) {
      handler.addError("PRMapper requires a 'lambda' parameter.");
    }
  }
}
