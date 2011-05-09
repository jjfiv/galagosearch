// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.ngram;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.galagosearch.core.types.NgramFeature;
import org.galagosearch.tupleflow.FileOrderedWriter;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * 
 * Writes NgramFeatures to a file in a specified folder.
 * Uses the internal NgramFeature Ordered Writer
 * 
 * This class is used within the Space Efficient Ngram Indexer
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.NgramFeature")
public class NgramFeatureWriter implements Processor<NgramFeature> {
  FileOrderedWriter<NgramFeature> writer;
  
  public NgramFeatureWriter(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException{
    File folder = new File(parameters.getXML().get("filterFolder"));
    writer =  getTemporaryWriter(folder + File.separator + "features." + parameters.getInstanceId());
  }
  
  private FileOrderedWriter<NgramFeature> getTemporaryWriter(String file) throws IOException {
    FileOrderedWriter<NgramFeature> tempWriter = new FileOrderedWriter<NgramFeature>(file, new NgramFeature.FileFilePositionOrder());
    return tempWriter;
  }

  long count = 0;
  public void process(NgramFeature ngram) throws IOException {
    writer.process(ngram);
    count++;
  }
  
  public void close() throws IOException{
    System.err.println("Locations Writer:" + count);
    writer.close();
  }
}
