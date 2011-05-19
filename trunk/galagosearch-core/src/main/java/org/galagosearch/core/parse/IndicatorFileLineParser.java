/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.parse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.galagosearch.tupleflow.Counter;
import org.galagosearch.tupleflow.ExNihiloSource;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.Linkage;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@OutputClass(className = "java.lang.String")
public class IndicatorFileLineParser implements ExNihiloSource<String> {
  public Processor<String> processor;
  Parameters p;
  Counter lines;
  
  public IndicatorFileLineParser(TupleFlowParameters parameters){
    p = parameters.getXML();
    lines = parameters.getCounter("Indicator Lines Read");
  }
  
  public void run() throws IOException {
    BufferedReader reader;
    for(String f : p.stringList( "input" )){
      reader = new BufferedReader( new FileReader( f ) );
      String line;
      while(null != (line = reader.readLine())){
        processor.process(line);
      }
      reader.close();
    }
    processor.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }
 
}
