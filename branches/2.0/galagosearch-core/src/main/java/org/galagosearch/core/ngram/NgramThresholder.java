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
 * Discards n-grams that contain words that
 * occur at least <threshold> times within the corpus.
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.NumberWordPosition", order = {"+word", "+document", "+position"})
@OutputClass(className = "org.galagosearch.core.types.NumberWordPosition", order = {"+word", "+document", "+position"})
public class NgramThresholder extends StandardStep<NumberWordPosition, NumberWordPosition> {
  int threshold;
  ArrayList<NumberWordPosition> current;
  
  public NgramThresholder(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException{
    threshold = Integer.parseInt(parameters.getXML().get("threshold"));
    current = new ArrayList();
  }

  public void process(NumberWordPosition ngram) throws IOException {
    if ((current.size() > 0) &&
        (Utility.compare(ngram.word, current.get(0).word) == 0)){
      current.add(ngram);
    } else {
      if(current.size() >= threshold){
        for(NumberWordPosition i : current){
          processor.process( i );
        }
      }
      current.clear();
      current.add(ngram);
    }
  }
  
  public void close() throws IOException{
    if(current.size() >= threshold){
      for(NumberWordPosition i : current){
        processor.process( i );
      }
    }
    processor.close();
  }
}
