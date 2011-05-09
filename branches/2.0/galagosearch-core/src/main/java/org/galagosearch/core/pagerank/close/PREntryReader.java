// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.pagerank.close;

import org.galagosearch.core.types.NumberedLink;
import org.galagosearch.core.pagerank.iter.PRDoc;
import org.galagosearch.core.types.PREntry;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.FileOrderedReader;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;

import java.io.File;
import java.io.IOException;

/**
 *  Reads pagerank values from the temporary directory
 *  
 * @author sjh
 */
@OutputClass(className = "org.galagosearch.core.types.PREntry", order={"+docNum"})
public class PREntryReader implements ExNihiloSource<PREntry> {

  private FileOrderedReader<PREntry> entriesReader;
  public Processor<PREntry> processor;

  public PREntryReader(TupleFlowParameters params) throws IOException {
    String entriesFilename;
    entriesFilename = params.getXML().get("entries");
    entriesReader = new FileOrderedReader<PREntry>(entriesFilename, new PREntry.DocNumOrder());
  }

  public void run() throws IOException {
    PREntry curr = entriesReader.read();
    while(curr != null){
      processor.process(curr);
      curr = entriesReader.read();
    }

    close();
  }

  /**
   * Need this to close the file.
   */
  public void close() throws IOException {
    entriesReader.close();
    processor.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }


  /**
   * Makes sure that the TupleFlow parameter file passes a parameter called
   * 'pagerankTemp' to this class.
   */
  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("entries")) {
      handler.addError("PRReader requires a 'entries' parameter.");
    }
  }
}
