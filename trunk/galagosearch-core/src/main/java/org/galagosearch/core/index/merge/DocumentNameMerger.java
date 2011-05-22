// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.merge;

import java.io.IOException;
import java.util.List;
import org.galagosearch.core.index.DocumentLengthsReader;
import org.galagosearch.core.index.DocumentNameReader;
import org.galagosearch.core.index.DocumentNameWriter;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentNameMerger extends GenericIndexMerger<NumberedDocumentData> {

  public DocumentNameMerger(TupleFlowParameters p) throws Exception {
    super(p);
    this.mappingKeys = true;
  }

  @Override
  public Processor<NumberedDocumentData> createIndexWriter(TupleFlowParameters parameters) throws Exception {
    return new DocumentNameWriter(parameters);
  }

  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {
    assert (keyIterators.size() == 1) : "Found two identical keys when merging names. Name data should never be combined.";
    DocumentNameReader.KeyIterator i = (DocumentNameReader.KeyIterator) keyIterators.get(0).iterator;
    this.writer.process(new NumberedDocumentData(i.getCurrentName(), null, Utility.toInt(key), 0));
  }
}
