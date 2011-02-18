// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.iter;

import org.galagosearch.core.types.PREntry;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.execution.Verified;

import java.io.IOException;

/**
 * accumulates the pagerank value for a given document id
 * 
 * @author schiu, sjh
 *
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.PREntry", order = {"+docNum"})
@OutputClass(className = "org.galagosearch.core.types.PREntry", order = {"+docNum"})
public class PRReducer extends StandardStep<PREntry, PREntry> {

  private PREntry currentEntry = null;

  public void process(PREntry entry) throws IOException {
    if(currentEntry == null){
      currentEntry = entry;
    } else if (currentEntry.docNum == entry.docNum) {
      currentEntry.score += entry.score;
    } else {
      processor.process(currentEntry);
      currentEntry = entry;
    }
  }

  /**
   * Need to output the last entry.
   */
  public void close() throws IOException {
    processor.process(currentEntry);
    processor.close();
  }
}
