// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.types.DocumentIndicator;
import org.galagosearch.core.types.NumberWordProbability;
import org.galagosearch.tupleflow.Counter;
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

  int lastDocument = -1;
  double maxObservedScore = Double.NEGATIVE_INFINITY;
  double minObservedScore = Double.POSITIVE_INFINITY;

  double minWrittenScore = Math.log(0.0000000001);
  
  Counter written;

  /** Creates a new instance of DocumentLengthsWriter */
  public DocumentPriorWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    super(parameters, "Document indicators written");
    Parameters p = writer.getManifest();
    p.set("writerClass", DocumentPriorWriter.class.getName());
    p.set("readerClass", DocumentPriorReader.class.getName());

    // ensure we set a default value - default default value is 'false'
    minWrittenScore = parameters.getXML().get("default", minWrittenScore);

    written = parameters.getCounter("Priors Written");
  }

  public GenericElement prepare(NumberWordProbability nwp) throws IOException {
    // word is ignored
    assert ((lastDocument < 0) || (lastDocument < nwp.number)) : "DocumentPriorWriter keys must be unique and in sorted order.";
    
    maxObservedScore = Math.max(maxObservedScore, nwp.probability);
    minObservedScore = Math.min(minObservedScore, nwp.probability);
    GenericElement element = new GenericElement(Utility.fromInt(nwp.number), Utility.fromDouble(nwp.probability));

    if( nwp.probability > minWrittenScore ){
      if(written != null) written.increment();
      return element;
    } else {
      return null;
    }
  }

  public void close() throws IOException {
    Parameters p = writer.getManifest();
    p.set("maxScore", Double.toString(this.maxObservedScore));
    p.set("minScore", Double.toString(this.minObservedScore));
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
