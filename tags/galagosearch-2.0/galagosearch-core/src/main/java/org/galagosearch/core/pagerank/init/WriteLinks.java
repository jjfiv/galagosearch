// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.init;

import org.galagosearch.core.types.NumberedLink;
import org.galagosearch.tupleflow.FileOrderedWriter;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.TupleFlowParameters;

import java.io.IOException;
import java.io.File;
import org.galagosearch.tupleflow.Utility;

/**
 * Writes extracted links (docnumber-docnumber pairs)
 * to file to be used in the main pagerank iteration
 * 
 * @author schiu, sjh
 *
 */
@InputClass(className = "org.galagosearch.core.types.NumberedLink", order = {"+source"})
public class WriteLinks implements Processor<NumberedLink> {

  private Counter linkCounter;
  public FileOrderedWriter<NumberedLink> out;

  public WriteLinks(TupleFlowParameters params) throws IOException {
    String filename = params.getXML().get("links");
    Utility.makeParentDirectories(filename); // makes the prtmp folder as rqeuired

    linkCounter = params.getCounter("Links Written");
    out = new FileOrderedWriter<NumberedLink>(filename, new NumberedLink.SourceOrder(), true);
  }

  NumberedLink last = null;
  public void process(NumberedLink el) throws IOException {
    assert( last == null || last.source <= el.source) : "Links are being written out of order.";
    out.process(el);
    if (linkCounter != null) {
      linkCounter.increment();
    }
  }

  public void close() throws IOException {
    out.close();
  }

  /**
   * Makes sure that the TupleFlow parameter file passes a parameter called
   * 'filename' to this class.
   */
  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("links")) {
      handler.addError("WriteLinks requires a 'links' parameter.");
    }
  }
}
