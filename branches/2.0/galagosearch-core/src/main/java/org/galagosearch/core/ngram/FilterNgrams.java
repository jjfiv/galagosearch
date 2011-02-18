// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.ngram;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.galagosearch.core.types.NgramFeature;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OrderedCombiner;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Filter n-grams uses a filter (all files with a specified folder)
 * that contains locations of potentially useful ngrams
 * 
 * Useful is defined here as n-gram occurs in corpus at least
 * <threshold> times.
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.ngram.Ngram")
@OutputClass(className = "org.galagosearch.core.ngram.Ngram")
public class FilterNgrams extends StandardStep<Ngram, Ngram> {
  //TypeReader<NgramFeature> filterStream;
  OrderedCombiner<NgramFeature> filterStream;
  NgramFeature filterHead;

  
  public FilterNgrams(TupleFlowParameters parameters) throws IOException {
    //String filterStreamName = parameters.getXML().get("filterStream");
    //filterStream = parameters.getTypeReader( filterStreamName );

    filterStream = getFilterReader(parameters);
    filterHead = filterStream.read();
  }

  public OrderedCombiner<NgramFeature> getFilterReader(TupleFlowParameters parameters) throws IOException{
    File inputFolder = new File(parameters.getXML().get("filterFolder"));    
    ArrayList<String> filenames = new ArrayList<String>();
    for(File f : inputFolder.listFiles()){
      filenames.add(f.getAbsolutePath());
    }
    return OrderedCombiner.combineFromFiles(filenames, new NgramFeature.FileFilePositionOrder());
  }
  
  long dropCount = 0;
  long totalCount = 0;
  public void process(Ngram n) throws IOException {
    totalCount++;
    // skip filterstream to the correct file
    while ((filterHead != null) &&
        (filterHead.file < n.file)){
      filterHead = filterStream.read();
    }
    
    // skip filterstream to the correct filePosition
    while ((filterHead != null) && 
        (filterHead.file == n.file) &&
        (filterHead.filePosition < n.filePosition)){
      filterHead = filterStream.read();
    }

    // if this ngram is in the correct position -- process it
    if ((filterHead != null) && 
        (filterHead.file == n.file) &&
        (filterHead.filePosition == n.filePosition)){
      processor.process( n );
    } else {
      // otherwise ignore it
      dropCount++;
    }
  }
  
  public void close() throws IOException {
    System.err.println("Dropped " + dropCount + " of " + totalCount);
    processor.close();
  }
}
