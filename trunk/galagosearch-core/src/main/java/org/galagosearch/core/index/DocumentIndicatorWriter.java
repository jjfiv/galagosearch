// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.index.merge.DocumentLengthsMerger;
import org.galagosearch.core.types.DocumentIndicator;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;

/**
 * Writes the document indicator file 
 * 
 * @author sjh
 */
@InputClass(className = "org.galagosearch.core.types.DocumentIndicator", order = {"+document"})
public class DocumentIndicatorWriter extends KeyValueWriter<DocumentIndicator> {

  int lastDocument = -1;

  /** Creates a new instance of DocumentLengthsWriter */
  public DocumentIndicatorWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    super(parameters, "Document indicators written");
    Parameters p = writer.getManifest();
    p.set("writerClass", DocumentIndicatorWriter.class.getName());
    p.set("readerClass", DocumentIndicatorReader.class.getName());

    // ensure we set a default value - default default value is 'false'
    p.set("default", parameters.getXML().get("default", "false"));
  }

  public GenericElement prepare(DocumentIndicator di) throws IOException {
    assert ((lastDocument < 0) || (lastDocument < di.document)) : "DocumentIndicatorWriter keys must be unique and in sorted order.";
    GenericElement element = new GenericElement(Utility.fromInt(di.document), Utility.fromBoolean(di.indicator));
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
