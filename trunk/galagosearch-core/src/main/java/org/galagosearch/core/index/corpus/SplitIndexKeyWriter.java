/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index.corpus;

import java.io.File;
import java.io.IOException;
import org.galagosearch.core.index.GenericElement;
import org.galagosearch.core.index.IndexWriter;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 * Split index key writer
 *  - Index is a mapping from byte[] to byte[]
 *
 *  - allows values to be written out of order to a set of files
 *  - a unified ordered key structure should be kept in a folder
 *    with these value files, as created by SplitIndexKeyWriter
 *  - SplitIndexReader will read this data
 *
 *  This class if useful for writing a corpus structure
 *  - documents can be written to disk in any order
 *  - the key structure allows the documents to be found quickly
 *  - class is more efficient if the
 *    documents are inserted in sorted order
 *
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.types.KeyValuePair")
public class SplitIndexKeyWriter implements Processor<KeyValuePair> {

    IndexWriter writer;
    private Counter keyCounter;

    public SplitIndexKeyWriter(TupleFlowParameters parameters) throws IOException{
        String file = parameters.getXML().get("filename") + File.separator + "key.index";
        Utility.makeParentDirectories(file);
        writer = new IndexWriter( file , parameters.getXML() );
	keyCounter = parameters.getCounter("Document Keys Written");
    }

    public void process(KeyValuePair object) throws IOException {
        writer.add( new GenericElement( object.key, object.value ) );
	if (keyCounter != null) keyCounter.increment();
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
