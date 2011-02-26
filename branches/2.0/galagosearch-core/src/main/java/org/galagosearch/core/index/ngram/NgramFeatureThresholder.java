// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index.ngram;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.galagosearch.core.types.NgramFeature;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Discards NgramFeature items that contain words that
 * occur at least <threshold> times within the corpus.
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.NgramFeature")
@OutputClass(className = "org.galagosearch.core.types.NgramFeature")
public class NgramFeatureThresholder extends StandardStep<NgramFeature, NgramFeature> {

  int threshold;
  ArrayList<NgramFeature> current;
  
  public NgramFeatureThresholder(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException{
    threshold = Integer.parseInt(parameters.getXML().get("threshold"));
    current = new ArrayList();
  }

  public void process(NgramFeature ngram) throws IOException {
    if ((current.size() > 0) &&
        (Utility.compare(ngram.feature, current.get(0).feature) == 0)){
      current.add(ngram);
    } else {
      if(current.size() >= threshold){
        for(NgramFeature n : current){
          processor.process( n );
        }
      }
      current.clear();
      current.add(ngram);
    }
  }
  
  public void close() throws IOException{
    if(current.size() >= threshold){
      for(NgramFeature n : current){
        processor.process( n );
      }
    }
    processor.close();
  }
}
