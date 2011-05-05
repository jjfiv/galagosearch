// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.window;

import java.io.IOException;

import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * 
 * Converts Ngram objects to NumberWordPosition objects
 * Some data is discarded
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.window.Window")
@OutputClass(className = "org.galagosearch.core.types.NumberedExtent")
public class WindowToNumberedExtent extends StandardStep<Window, NumberedExtent> {

  public void process(Window window) throws IOException {
    processor.process(
       new NumberedExtent(window.data, window.document, window.begin, window.end));
  }
}
