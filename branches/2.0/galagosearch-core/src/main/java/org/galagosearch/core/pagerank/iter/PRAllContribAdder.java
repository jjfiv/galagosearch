// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.iter;

import java.io.DataInputStream;
import java.io.IOException;

import org.galagosearch.core.types.PREntry;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.StreamCreator;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/**
 * Adds the total random jump score to each document's pagerank value
 * 
 * @author schiu, sjh
 *
 */
@InputClass(className = "org.galagosearch.core.types.PREntry", order = {"+docNum"})
@OutputClass(className = "org.galagosearch.core.types.PREntry", order = {"+docNum"})
public class PRAllContribAdder extends StandardStep<PREntry, PREntry> {

  private double scoreToAll;

  public PRAllContribAdder(TupleFlowParameters parameters) throws IOException {
    String filename = parameters.getXML().get("allContrib");
    DataInputStream in = new DataInputStream(StreamCreator.realInputStream(filename));
    scoreToAll = in.readDouble();
    in.close();
  }

  public void process(PREntry entry) throws IOException {
    entry.score += scoreToAll;
    processor.process(entry);
  }

  /**
   * Makes sure that the TupleFlow parameter file passes a parameter called
   * 'tempDir' to this class.
   */
  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("allContrib")) {
      handler.addError("WritePRENtries requires a 'allContrib' parameter.");
    }
  }
}
