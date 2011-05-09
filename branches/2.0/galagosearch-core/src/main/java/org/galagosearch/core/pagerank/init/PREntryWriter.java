// BSD License (http://www.galagosearch.org/license) 
package org.galagosearch.core.pagerank.init;

import org.galagosearch.core.types.PREntry;
import org.galagosearch.tupleflow.FileOrderedWriter;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.TupleFlowParameters;

import java.io.File;
import java.io.IOException;
import org.galagosearch.tupleflow.Utility;

/**
 * Writes pagerank entries to disk
 *  <doc-id, pagerank-value>
 *  
 * used in init and iter for pagerank algorithm
 * 
 * @author schiu, sjh
 *
 */
@InputClass(className = "org.galagosearch.core.types.PREntry", order = {"+docNum"})
public class PREntryWriter implements Processor<PREntry> {

  private Counter entryCounter;
  private FileOrderedWriter<PREntry> out;

  public PREntryWriter(TupleFlowParameters params) throws IOException {

    String filename = params.getXML().get("entries");
    Utility.makeParentDirectories(filename); // makes the prtmp folder as rqeuired
    if(new File(filename).exists()) new File(filename).delete(); // remove the file if it exists

    out = new FileOrderedWriter<PREntry>(filename, new PREntry.DocNumOrder(), true);
    entryCounter = params.getCounter("PR Entries Written");
  }

  PREntry last = null;
  public void process(PREntry pre) throws IOException {
    assert( last == null || last.docNum < pre.docNum) : "WritePREntry.class: document entries are out of order" ;
    out.process(pre);
    entryCounter.increment();
  }

  public void close() throws IOException {
    out.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("entries")) {
      handler.addError("WritePRENtries requires a 'entries' parameter.");
    }
  }
}
