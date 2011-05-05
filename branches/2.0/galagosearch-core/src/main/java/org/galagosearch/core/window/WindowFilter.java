// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.window;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.core.types.TextFeature;
import org.galagosearch.tupleflow.Counter;

import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OrderedCombiner;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * WindowFilter uses a filter that contains locations of
 * potentially frequent windows
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.window.Window")
@OutputClass(className = "org.galagosearch.core.window.Window")
public class WindowFilter extends StandardStep<Window, Window> {
  TypeReader<TextFeature> filterStream;
  TextFeature filterHead;

  long dropCount = 0;
  long totalCount = 0;

  Counter dropped;
  Counter passed;

  public WindowFilter(TupleFlowParameters parameters) throws IOException {
    String filterStreamName = parameters.getXML().get("filterStream");
    filterStream = parameters.getTypeReader( filterStreamName );
    filterHead = filterStream.read();

    dropped = parameters.getCounter("Windows Dropped");
    passed = parameters.getCounter("Windows Passed");
  }
  
  public void process(Window w) throws IOException {
    totalCount++;

    // skip filterstream to the correct file
    while ((filterHead != null) &&
        (filterHead.file < w.file)){
      filterHead = filterStream.read();
    }
    
    // skip filterstream to the correct filePosition
    while ((filterHead != null) && 
        (filterHead.file == w.file) &&
        (filterHead.filePosition < w.filePosition)){
      filterHead = filterStream.read();
    }

    // if this window is in the correct position -- process it
    if ((filterHead != null) && 
        (filterHead.file == w.file) &&
        (filterHead.filePosition == w.filePosition)){
      processor.process( w );
      if (passed != null) passed.increment();
    } else {
      // otherwise ignore it
      dropCount++;
      if(dropped != null) dropped.increment();
    }
  }
  
  public void close() throws IOException {
    System.err.println("Dropped " + dropCount + " of " + totalCount);
    processor.close();
  }
}
