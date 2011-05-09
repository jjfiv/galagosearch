// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.init;

import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.HalfNumberedLink;
import org.galagosearch.core.types.NumberedLink;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.tupleflow.IncompatibleProcessorException;

import java.io.IOException;

/*
 * replaces source urls with internal document ids
 * 
 * @author schiu, sjh 
 */
@Verified
@OutputClass(className = "org.galagosearch.core.types.NumberedLink")
public class FinishNumbering implements ExNihiloSource<NumberedLink> {

  private TypeReader<NumberedDocumentData> numberedDataReader;
  private TypeReader<HalfNumberedLink> sortedLinksReader;
  public Processor<NumberedLink> processor;

  public FinishNumbering(TupleFlowParameters parameters) throws IOException {
    numberedDataReader = parameters.getTypeReader(parameters.getXML().get("numberedData"));
    sortedLinksReader = parameters.getTypeReader(parameters.getXML().get("sortedLinks"));
  }

  /**
   * This is the only method we need.
   */
  public void run() throws IOException {
    NumberedDocumentData numberedData = numberedDataReader.read();
    HalfNumberedLink sortedLink;

    while ((sortedLink = sortedLinksReader.read()) != null) {

      while ((numberedData != null) && (numberedData.url.compareTo(sortedLink.src) < 0)) {
        numberedData = numberedDataReader.read();
      }
      if ((numberedData != null) && (numberedData.url.equals(sortedLink.src))) {
        processor.process(new NumberedLink(numberedData.number, sortedLink.dest));
      }
      if (numberedData == null) {
        //System.out.println("Src: " + sortedLink.src);
        break;
      }
    }
    processor.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }
}
