// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.iter;

import org.galagosearch.core.types.NumberedLink;
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
 * reads links and previous pagerank values files in parallel
 * PRdocs with a pr value and a set of links are produced 
 * 
 * @author schiu, sjh
 *
 */
@OutputClass(className = "org.galagosearch.core.pagerank.iter.PRDoc")
public class PRReader implements ExNihiloSource<PRDoc> {

  private FileOrderedReader<PREntry> entriesReader;
  private FileOrderedReader<NumberedLink> linksReader;
  public Processor<PRDoc> processor;
  private Counter entryCounter;
  private Counter linkCounter;

  public PRReader(TupleFlowParameters params) throws IOException {
    String entriesFilename;
    String linksFilename;

    entriesFilename = params.getXML().get("entries");
    linksFilename = params.getXML().get("links");

    entriesReader = new FileOrderedReader<PREntry>(entriesFilename, new PREntry.DocNumOrder());
    linksReader = new FileOrderedReader<NumberedLink>(linksFilename, new NumberedLink.SourceOrder());

    entryCounter = params.getCounter("Entries Read In");
    linkCounter = params.getCounter("Links Read In");
  }

  public void run() throws IOException {
    PRDoc currDoc = new PRDoc(entriesReader.read());
    if(entryCounter != null) entryCounter.increment();
    NumberedLink currLink = linksReader.read();
    if(linkCounter != null) linkCounter.increment();

    while (currDoc.entry != null) {
      while ((currLink != null) && (currLink.source == currDoc.entry.docNum)) {
        currDoc.add(currLink);
        if(linkCounter != null) linkCounter.increment();
        currLink = linksReader.read();
      }

      processor.process(currDoc);
      if(entryCounter != null) entryCounter.increment();
      currDoc = new PRDoc(entriesReader.read());
    }
    close();
  }

  /**
   * Need this to close the file.
   */
  public void close() throws IOException {
    entriesReader.close();
    linksReader.close();
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
    if (!parameters.getXML().containsKey("links")) {
      handler.addError("PRReader requires a 'links' parameter.");
    }
  }
}
