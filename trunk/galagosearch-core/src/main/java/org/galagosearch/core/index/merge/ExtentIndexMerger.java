/*
 *  BSD License (http://www.galagosearch.org/license)
 */

package org.galagosearch.core.index.merge;

import java.io.IOException;
import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.retrieval.structured.Extent;
import org.galagosearch.core.types.NumberedExtent;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;

/**
 *
 * @author sjh
 */
public class ExtentIndexMerger extends GenericExtentValueIndexMerger<NumberedExtent> {

  public ExtentIndexMerger(TupleFlowParameters parameters) throws Exception{
    super(parameters);
  }

  @Override
  public boolean mappingKeys() {
    return false;
  }
  
  @Override
  public Processor<NumberedExtent> createIndexWriter(TupleFlowParameters parameters) throws Exception {
    ExtentIndexWriter w = new ExtentIndexWriter(parameters);
    NumberedExtent.ExtentNameNumberBeginOrder.TupleShredder shredder = new NumberedExtent.ExtentNameNumberBeginOrder.TupleShredder( w );
    return shredder;
  }
  
  public void transformExtentArray(byte[] key, ExtentArray extentArray)  throws IOException {
    for(int i=0; i <extentArray.getPositionCount() ; i++){
      Extent e = extentArray.getBuffer()[i];
      this.writer.process(new NumberedExtent(key, e.document, e.begin, e.end) );
    }
  }
}
