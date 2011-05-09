// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.iter;

import java.io.File;

import org.galagosearch.core.types.PREntry;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.FileOrderedReader;

import java.lang.Math;
import java.io.IOException;

/**
 * Determines if the algorithm has converged
 * delta is the largest single change in pagerank value during  this iteration
 * 
 * @author schiu, sjh
 *
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.PREntry", order = {"+docNum"})
@OutputClass(className = "org.galagosearch.core.types.PREntry", order = {"+docNum"})
public class PRDeltaCalculator extends StandardStep<PREntry, PREntry> {

  private FileOrderedReader<PREntry> entriesReader;
  private File converganceFile;
  private double convergenceThreshold;
  private double sum = 0.0;
  private double highestDelta = 0.0;

  public PRDeltaCalculator(TupleFlowParameters parameters) throws IOException {
    convergenceThreshold = Double.parseDouble(parameters.getXML().get("convergenceThreshold"));

    converganceFile = new File(parameters.getXML().get("converged"));
    if (converganceFile.exists()) {
      converganceFile.delete();
    }

    String entriesFilename = parameters.getXML().get("entries");
    entriesReader = new FileOrderedReader<PREntry>(entriesFilename, new PREntry.DocNumOrder());
  }

  public void process(PREntry entry) throws IOException {
    PREntry oldEntry = entriesReader.read();

    assert (entry.score > 0.0) :
            "Something has gone wrong\n "
            + "we have a zero score :"
            + entry.docNum + " - " + entry.score;

    assert (entry.docNum == oldEntry.docNum) :
            "Something has gone wrong in the entries file\n "
            + "we appear to be missing one : "
            + entry.docNum + " - " + oldEntry.docNum;

    highestDelta = Math.max(Math.abs(oldEntry.score - entry.score), highestDelta);

    processor.process(entry);
  }

  public void close() throws IOException {
    if (highestDelta < convergenceThreshold) {
      converganceFile.createNewFile();
    }
    processor.close();
  }

  /**
   * Makes sure that the TupleFlow parameter file passes a parameter called
   * 'tempDir' to this class.
   */
  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("converged")) {
      handler.addError("PRDeltaCalculator requires a 'converged' parameter.");
    }
    if (!parameters.getXML().containsKey("entries")) {
      handler.addError("PRDeltaCalculator requires a 'entries' parameter.");
    }
    if (!parameters.getXML().containsKey("convergenceThreshold")) {
      handler.addError("PRDeltaCalculator requires a 'convergenceThreshold' parameter.");
    }
  }
}
