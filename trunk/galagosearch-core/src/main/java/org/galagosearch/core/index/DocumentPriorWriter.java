// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.types.DocumentIndicator;
import org.galagosearch.core.types.NumberWordProbability;
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
@InputClass(className = "org.galagosearch.core.types.NumberWordProbability", order = {"+number"})
public class DocumentPriorWriter extends KeyValueWriter<NumberWordProbability> {

  int document = 0;
  int offset = 0;
  ByteArrayOutputStream bstream;
  DataOutputStream stream;
  
  double maxScore = Double.NEGATIVE_INFINITY;
  double minScore = Double.POSITIVE_INFINITY;

  
  /** Creates a new instance of DocumentLengthsWriter */
  public DocumentPriorWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    super(parameters, "Document indicators written");
    Parameters p = writer.getManifest();
    p.set("writerClass", DocumentPriorWriter.class.getName());
    p.set("readerClass", DocumentPriorReader.class.getName());

    // ensure we set a default value - default default value is 'false'
    p.set("default", parameters.getXML().get("default", "-inf"));
    
    bstream = new ByteArrayOutputStream();
    stream = new DataOutputStream(bstream);
  }

  public GenericElement prepare(NumberWordProbability nwp) throws IOException {
    // word is ignored
    maxScore = Math.max(maxScore, nwp.probability);
    minScore = Math.min(minScore, nwp.probability);
    GenericElement element = new GenericElement(Utility.fromInt(nwp.number), Utility.fromDouble(nwp.probability));
    return element;
  }

  public void close() throws IOException{
    Parameters p = writer.getManifest();
    p.set("maxScore", Double.toString(this.maxScore));
    p.set("minScore", Double.toString(this.minScore));
    super.close();
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
