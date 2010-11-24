// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.iter;

import java.io.File;
import java.io.DataOutputStream;

import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.execution.ErrorHandler;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.tupleflow.StreamCreator;

/**
 * Calculates the total random jump score for the set of documents
 * Writes value to manifest file in pagerank tempdir
 * 
 * @author schiu, sjh
 *
 */
@InputClass(className = "org.galagosearch.core.pagerank.iter.PRDoc")
@OutputClass(className = "org.galagosearch.core.pagerank.iter.PRDoc")
public class PRAllContribCollector extends StandardStep<PRDoc, PRDoc> {

  private DataOutputStream out;
  private double sum = 0.0;
  private double count;
  //chance of jumping to a random page
  private double lambda;

  public PRAllContribCollector(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    lambda = Double.parseDouble(parameters.getXML().get("lambda"));
    Parameters manifest = new Parameters(new File(parameters.getXML().get("manifest")));
    count = Long.parseLong(manifest.get("documentCount"));

    File outFile = new File(parameters.getXML().get("allContrib"));
    if (outFile.exists()) outFile.delete();

    out = StreamCreator.realOutputStream(outFile.getAbsolutePath());
  }

  public void process(PRDoc doc) throws IOException {
    if(doc.size() > 0){
      sum += doc.entry.score * lambda;
    } else {
      sum += doc.entry.score;
    }
    processor.process(doc);
  }

  public void close() throws IOException {
    double contrib = sum / count;
    out.writeDouble(contrib);
    out.close();
    processor.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("manifest")) {
      handler.addError("PRAllContribCollector requires a 'manifest' parameter containing docCounts.");
    }
    if (!parameters.getXML().containsKey("lambda")) {
      handler.addError("PRAllContribCollector requires a 'lambda' parameter.");
    }
    if (!parameters.getXML().containsKey("allContrib")) {
      handler.addError("PRAllContribCollector requires a 'allContrib' parameter.");
    }
  }
}
