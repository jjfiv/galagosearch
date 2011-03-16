/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index;

import java.io.File;
import java.io.IOException;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Source;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;
import org.galagosearch.tupleflow.types.XMLFragment;

/**
 *
 * @author irmarc
 */
public class ParameterWriter implements Source<XMLFragment> {

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    // We're not connecting this to anything
  }

  public ParameterWriter(TupleFlowParameters parameters) throws IOException {
    Parameters p = parameters.getXML();
    String target = p.get("filename");
    Parameters cloned = p.clone();
    cloned.set("filename", "");
    cloned.write(target);
  }

  public void run() throws IOException {
    // no op?
  }


  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("filename")) {
      handler.addError("ParameterWriter requires a 'filename' parameter.");
      return;
    }

    File f = new File(parameters.getXML().get("filename"));

    if (f.isFile() && f.canWrite()) {
      return; // good news
    }
    if (f.isDirectory()) {
      handler.addError("Pathname " + f.toString() + " exists, and it is a directory, but "
              + "Writer would like to write a file there.");
      return;
    }

    // this will search upwards and verify that we can make
    // the necessary directory structure to store this file.
    Verification.requireWriteableFile(f.toString(), handler);
  }
}
