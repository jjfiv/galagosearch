// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.ngram;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Discards NumberWordPosition items that contain words that
 * occur less than <threshold> times within the corpus.
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.NumberWordPosition", order={"+word", "+document", "+position"})
@OutputClass(className = "org.galagosearch.core.types.NumberWordPosition", order={"+word", "+document", "+position"}) 
public class NumberWordPositionThresholder extends StandardStep<NumberWordPosition, NumberWordPosition>
  implements NumberWordPosition.Source {

  long debug_count = 0;
  long debug_total_count = 0;
  
  int threshold;
  ArrayList<NumberWordPosition> current;
  
  public NumberWordPositionThresholder(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException{
    threshold = Integer.parseInt(parameters.getXML().get("threshold"));
    current = new ArrayList();
  }

  public void process(NumberWordPosition nwp) throws IOException {
    debug_total_count++;
    
    if ((current.size() > 0) &&
        (Utility.compare(nwp.word, current.get(0).word) == 0)){
      current.add(nwp);
    } else {
      if(current.size() >= threshold){
        for(NumberWordPosition n : current){
          processor.process( n ); 
          debug_count++;
        }
      }
      current.clear();
      current.add(nwp);
    }
  }
  
  public void close() throws IOException{
    if(current.size() >= threshold){
      for(NumberWordPosition n : current){
        processor.process( n );
        debug_count++;
      }
    }
    //System.err.println(debug_count + " of "+ debug_total_count + " accepted");
    processor.close();
  }
}
