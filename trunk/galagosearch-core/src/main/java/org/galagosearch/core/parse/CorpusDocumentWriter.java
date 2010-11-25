// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.parse;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPOutputStream;

import org.galagosearch.core.types.CorpusIndexItem;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.StreamCreator;
import org.galagosearch.tupleflow.TupleFlowParameters;
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
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.types.CorpusIndexItem")
public class CorpusDocumentWriter extends StandardStep<Document, CorpusIndexItem> {

  DataOutputStream writer;
  File outputFile;
  long offset;
  boolean compressed;

  public CorpusDocumentWriter(TupleFlowParameters parameters) throws IOException{
    File folder = new File(parameters.getXML().get("filename"));
    folder.mkdirs();

    compressed = parameters.getXML().get("compressed", true);
    if(compressed){
      outputFile = File.createTempFile("CorpusDocumentStore.", ".cds.z", folder);
    } else {
      outputFile = File.createTempFile("CorpusDocumentStore.", ".cds", folder);
    }
    writer = StreamCreator.realOutputStream(outputFile.getAbsolutePath());
    offset = 0;

  }

  public void process(Document document) throws IOException {
    ByteArrayOutputStream array = new ByteArrayOutputStream();
    ObjectOutputStream output;
    if(compressed){
      output = new ObjectOutputStream( new GZIPOutputStream( array ));
    } else {
      output = new ObjectOutputStream( array );
    }
    output.writeObject(document);
    output.close();
    byte[] data = array.toByteArray();
    writer.write(data);

    CorpusIndexItem item = new CorpusIndexItem(document.identifier, outputFile.getName(), offset);

    offset += data.length;

    processor.process(item);
  }

  public void close() throws IOException {
    writer.close();
    if(offset == 0){
      // No files Written; deleting output file
      outputFile.delete();
    }
    processor.close();
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
