// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.galagosearch.core.index.merge.DocumentLengthsMerger;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 * Writes the document lengths file based on data in NumberedDocumentData tuples.
 * The document lengths data is used by StructuredIndex because it's a key
 * input to more scoring functions.
 * 
 * offset is the first document number (for sequential sharding purposes)
 *
 * (12/01/2010, irmarc): Rewritten to make use of the IndexWriter class. As it is, the memory-mapping is
 *                     fast, but its also dangerous due to lack of compression
 * 
 * @author trevor, sjh, irmarc
 */
@InputClass(className = "org.galagosearch.core.types.NumberedDocumentData", order = {"+number"})
public class DocumentLengthsWriter extends KeyValueWriter<NumberedDocumentData> {

  DataOutputStream output;
  int document = 0;
  int offset = 0;
  ByteArrayOutputStream bstream;
  DataOutputStream stream;

  /** Creates a new instance of DocumentLengthsWriter */
  public DocumentLengthsWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    super(parameters, "Document lengths written");
    Parameters p = writer.getManifest();
    p.set("writerClass", DocumentLengthsWriter.class.getName());
    p.set("mergerClass", DocumentLengthsMerger.class.getName());
    p.set("readerClass", DocumentLengthsReader.class.getName());

    bstream = new ByteArrayOutputStream();
    stream = new DataOutputStream(bstream);
  }

  public GenericElement prepare(NumberedDocumentData object) throws IOException {
    bstream.reset();
    Utility.compressInt(stream, object.textLength);
    GenericElement element = new GenericElement(Utility.fromInt(object.number), bstream.toByteArray());
    return element;
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("filename")) {
      handler.addError("KeyValueWriters require a 'filename' parameter.");
      return;
    }

    String index = parameters.getXML().get("filename");
    Verification.requireWriteableFile(index, handler);
  }
}
