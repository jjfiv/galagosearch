// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.galagosearch.core.index.GenericElement;
import org.galagosearch.core.index.IndexWriter;
import org.galagosearch.core.types.CorpusIndexItem;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.VByteOutput;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Writes documents to a file
 *  - new output file is created in the folder specified by "filename"
 *  - document.identifier -> output-file, byte-offset is passed on
 * 
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.types.CorpusIndexItem")
public class CorpusIndexWriter implements Processor<CorpusIndexItem> {
  IndexWriter writer;
  Counter documentsWritten;
  
  public CorpusIndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
      Parameters p = new Parameters();
      p.add("isCompressed", "true");
      String corpusIndexFile = parameters.getXML().get("filename") + File.separator + "index.corpus";
      writer = new IndexWriter(corpusIndexFile, p);
      documentsWritten = parameters.getCounter("Documents Written");
  }
  
  public void close() throws IOException {
      writer.close();
  }

  public void process(CorpusIndexItem cii) throws IOException {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      VByteOutput output = new VByteOutput(new DataOutputStream(stream));

      output.writeString(cii.filename);
      output.writeLong(cii.offset);
      
      writer.add(new GenericElement(cii.identifier, stream.toByteArray()));
      if (documentsWritten != null)
          documentsWritten.increment();
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
