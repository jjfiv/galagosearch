// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.merge;

import java.io.IOException;
import java.util.List;
import org.galagosearch.core.index.DocumentLengthsReader;
import org.galagosearch.core.index.DocumentLengthsWriter;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentLengthsMerger extends GenericIndexMerger<NumberedDocumentData> {

  public DocumentLengthsMerger(TupleFlowParameters p) throws Exception {
    super(p);
  }

  @Override
  public boolean mappingKeys() {
    return true;
  }

  @Override
  public Processor<NumberedDocumentData> createIndexWriter(TupleFlowParameters parameters) throws IOException {
    return new DocumentLengthsWriter(parameters);
  }
  
  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {
    assert( keyIterators.size() == 1 ) : "Found two identical keys when merging lengths. Length data should never be combined.";
    DocumentLengthsReader.KeyIterator i = (DocumentLengthsReader.KeyIterator) keyIterators.get(0).iterator;
    this.writer.process( new NumberedDocumentData(null, null, Utility.toInt(key), i.getCurrentLength()) );
  }
}
