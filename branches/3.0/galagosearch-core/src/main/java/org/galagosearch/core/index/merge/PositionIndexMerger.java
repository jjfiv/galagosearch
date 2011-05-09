/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index.merge;

import java.io.IOException;
import org.galagosearch.core.index.ExtentIndexWriter;
import org.galagosearch.core.index.PositionIndexWriter;
import org.galagosearch.core.retrieval.structured.Extent;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;

/**
 *
 * @author sjh
 */
public class PositionIndexMerger extends GenericExtentValueIndexMerger<NumberWordPosition> {

  public PositionIndexMerger(TupleFlowParameters parameters) throws Exception {
    super(parameters);
  }

  @Override
  public Processor<NumberWordPosition> createIndexWriter(TupleFlowParameters parameters) throws Exception {
    PositionIndexWriter w = new PositionIndexWriter(parameters);
    NumberWordPosition.WordDocumentPositionOrder.TupleShredder shredder = new NumberWordPosition.WordDocumentPositionOrder.TupleShredder(w);
    return shredder;
  }

  public void transformExtentArray(byte[] key, ExtentArray extentArray) throws IOException {
    for (int i = 0; i < extentArray.getPositionCount(); i++) {
      Extent e = extentArray.getBuffer()[i];
      this.writer.process( new NumberWordPosition( e.document, key, e.begin ) );
    }
  }
}
