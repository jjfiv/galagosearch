/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index.parallel;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.galagosearch.core.index.GenericElement;
import org.galagosearch.core.index.IndexWriter;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Source;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.StreamCreator;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 *
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.types.KeyValuePair")
public class ParallelIndexKeyWriter implements Processor<KeyValuePair> {

    IndexWriter writer;

    public ParallelIndexKeyWriter(TupleFlowParameters parameters) throws IOException{
        String file = parameters.getXML().get("filename") + File.separator + "key.index";
        Utility.makeParentDirectories(file);
        writer = new IndexWriter( file , parameters.getXML() );
    }

    public void process(KeyValuePair object) throws IOException {
        writer.add( new GenericElement( object.key, object.value ) );
    }

    public void close() throws IOException {
        writer.close();
    }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("filename")) {
      handler.addError("DocumentIndexWriter requires an 'filename' parameter.");
      return;
    }

    String index = parameters.getXML().get("filename");
    Verification.requireWriteableDirectory(index, handler);
  }
}
