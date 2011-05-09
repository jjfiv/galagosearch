// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.init;

import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.TupleFlowParameters;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import org.galagosearch.core.types.NumberedDocumentData;

/* 
 * Reformats urls extracted from documents
 * 
 * @author schiu, sjh
 */
@Verified
@InputClass(className="org.galagosearch.core.types.NumberedDocumentData" )
@OutputClass(className="org.galagosearch.core.types.NumberedDocumentData",order={"+url"})
public class DataFormatter extends StandardStep<NumberedDocumentData, NumberedDocumentData> {
  private Counter docCounter;

  public DataFormatter (TupleFlowParameters parameters) {
    docCounter = parameters.getCounter("Documents Formatted");
  }

  /** 
   * This is the only method we need.
   */
  public void process( NumberedDocumentData d) throws IOException {
    try{
      d.url = URLDecoder.decode(d.url);
    }
    catch(IllegalArgumentException i){
      //only for testing/debugging, safe to ignore in normal use
      //		System.err.println("URL:  " + d.url + "\ncaused error: " + i.getMessage());
    }		
    d.url = URLEncoder.encode(d.url);
    docCounter.increment();
    processor.process( d );

  }

}
