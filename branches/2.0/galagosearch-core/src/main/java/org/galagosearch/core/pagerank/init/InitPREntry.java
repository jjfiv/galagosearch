// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.init;

import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.PREntry;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.tupleflow.TupleFlowParameters;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.index.StructuredIndex;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;

/*
 * initial PR value writer
 *
 * @author schiu, sjh
 */
@InputClass(className = "org.galagosearch.core.types.NumberedDocumentData", order = {"+number"})
@OutputClass(className = "org.galagosearch.core.types.PREntry", order = {"+docNum"})
public class InitPREntry extends StandardStep<NumberedDocumentData, PREntry> {

  private double initScore;

  public InitPREntry(TupleFlowParameters parameters) throws FileNotFoundException, IOException {

    String index = parameters.getXML().get("index");
    StructuredIndex i = new StructuredIndex(index);
    long docCount = i.getDocumentCount();
    initScore = 1.0 / docCount;
    i.close();

    // write out the manifest file
    Parameters p = new Parameters();
    p.add("documentCount", Long.toString(docCount));
    p.write( parameters.getXML().get("manifest") );
  }

  public void process(NumberedDocumentData data) throws IOException {
    processor.process(new PREntry(data.number, initScore));
  }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("manifest")) {
      handler.addError("WritePRENtries requires a 'manifest' parameter.");
    }
  }

}
