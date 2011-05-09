// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.init;

import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.types.ExtractedLink;
import org.galagosearch.core.types.HalfNumberedLink;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Counter;

import java.io.IOException;

/**
 * filters extracted links to leave only internal links
 * also replaces destination urls with internal document ids 
 * 
 * @author schiu, sjh
 */
@Verified
@OutputClass(className = "org.galagosearch.core.types.HalfNumberedLink")
public class LinkFilter implements ExNihiloSource<ExtractedLink> {

  private TypeReader<NumberedDocumentData> srcLinksReader;
  private TypeReader<ExtractedLink> destLinksReader;
  public Processor<HalfNumberedLink> processor;
  public Counter links;

  public LinkFilter(TupleFlowParameters parameters) throws IOException {
    String srcLinksName = parameters.getXML().get("srcLinks");
    String destLinksName = parameters.getXML().get("destLinks");
    srcLinksReader = parameters.getTypeReader(srcLinksName);
    destLinksReader = parameters.getTypeReader(destLinksName);
    links = parameters.getCounter("Matching Links");
  }

  /**
   * This is the only method we need.
   */
  public void run() throws IOException {
    NumberedDocumentData src = srcLinksReader.read();
    ExtractedLink dest;

    while ((dest = destLinksReader.read()) != null) {

      while ((src != null) && (src.url.compareTo(dest.destUrl) < 0)) {
        src = srcLinksReader.read();
      }
      if ((src != null) && (src.url.equals(dest.destUrl)) && (!dest.srcUrl.equals(dest.destUrl))) {
        processor.process(new HalfNumberedLink(dest.srcUrl, src.number));
        links.increment();
      }
      if (src == null) {
        // ran out of src links
        // System.out.println("Dest: " + dest.srcUrl + "\n" + dest.destUrl);
        break;
      }
    }
    processor.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }
}
