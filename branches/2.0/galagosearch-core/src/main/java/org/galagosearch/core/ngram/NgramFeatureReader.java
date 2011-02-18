// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.ngram;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.galagosearch.core.types.NgramFeature;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.OrderedCombiner;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;


/**
 * n-gram Feature Reader reads all files in a folder.
 * 
 * It is not currently used by any program.
 *
 * @author sjh
 */

public class NgramFeatureReader implements TypeReader<NgramFeature>{
  OrderedCombiner<NgramFeature> reader;
  
  public NgramFeatureReader(TupleFlowParameters params) throws IOException{
    File inputFolder = new File(params.getXML().get("inputFolder"));    
    ArrayList<String> filenames = new ArrayList<String>();
    for(File f : inputFolder.listFiles()){
      filenames.add(f.getAbsolutePath());
    }
    reader = OrderedCombiner.combineFromFiles(filenames, new NgramFeature.FileFilePositionOrder());
  }

  public NgramFeatureReader(String folder) throws IOException{
    File inputFolder = new File(folder);
    ArrayList<String> filenames = new ArrayList<String>();
    for(File f : inputFolder.listFiles()){
      filenames.add(f.getAbsolutePath());
    }
    reader = OrderedCombiner.combineFromFiles(filenames, new NgramFeature.FileFilePositionOrder());
  }
  
  public NgramFeature getNext() throws IOException{
    return reader.read();
  }
  
  public void close() throws IOException{
    reader.close();
  }
  
  public static void main(String[] args) throws IOException{
    String fldr = args[0];
    NgramFeatureReader r = new NgramFeatureReader(fldr);
    NgramFeature f;
    while((f = r.getNext()) != null){
      System.err.println(f.file + "--" + f.filePosition);
    }
    r.close();
  }

  public NgramFeature read() throws IOException {
    return reader.read();
  }

  public void run() throws IOException {
    throw new IOException("NgramFeatureReader Not Implemented as a Processor");
  }

  public void setProcessor(Step processor)
      throws IncompatibleProcessorException {
    throw new IncompatibleProcessorException("Not Implemented as a Processor");
  }
}
