// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.window;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Discards NumberWordPosition items that contain words that
 * occur less than <threshold> times within the corpus.
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.NumberedExtent", order = {"+extentName", "+number", "+begin"})
@OutputClass(className = "org.galagosearch.core.types.NumberedExtent", order = {"+extentName", "+number", "+begin"})
public class NumberedExtentThresholder extends StandardStep<NumberedExtent, NumberedExtent>
        implements NumberedExtent.Source {

  long debug_count = 0;
  long debug_total_count = 0;
  int threshold;
  boolean threshdf;
  LinkedList<NumberedExtent> current;
  boolean currentPassesThreshold;

  Counter discards;
  Counter passing;

  public NumberedExtentThresholder(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException {
    threshold = Integer.parseInt(parameters.getXML().get("threshold"));
    threshdf = Boolean.parseBoolean(parameters.getXML().get("threshdf"));
    current = new LinkedList();

    discards = parameters.getCounter("Discarded Extents");
    passing = parameters.getCounter("Passed Extents");
  }

  public void process(NumberedExtent ne) throws IOException {
    debug_total_count++;

    if ((current.size() > 0)
            && (Utility.compare(ne.extentName, current.peekFirst().extentName) == 0)) {
      current.offerLast(ne);
      emitExtents();
    } else {
      emitExtents();
      if(discards != null) discards.incrementBy( current.size() );
      current.clear();
      current.offerLast(ne);
      currentPassesThreshold = false;
    }
  }

  private void emitExtents() throws IOException {

    // if we have more than threshold df
    if (threshdf) {
      HashSet<Long> docs = new HashSet();
      for(NumberedExtent e : current){
        docs.add(e.number);
      }
      if(docs.size() >= threshold){
        currentPassesThreshold = true;
      }
    } else {
      if (current.size() >= threshold) {
        currentPassesThreshold = true;
      }
    }

    // now actually emit Extents
    if (currentPassesThreshold) {
      while (current.size() > 0) {
        processor.process(current.pollFirst());
        if(passing != null) passing.increment();
      }
    }
  }

  public void close() throws IOException {
    emitExtents();
    //System.err.println(debug_count + " of "+ debug_total_count + " accepted");
    processor.close();
  }
}
