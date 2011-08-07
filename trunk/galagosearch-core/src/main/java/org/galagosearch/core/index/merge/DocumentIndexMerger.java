// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.merge;

import java.io.IOException;
import java.util.List;

import org.galagosearch.core.index.DocumentNameReader;
import org.galagosearch.core.index.corpus.DocumentIndexWriter;
import org.galagosearch.core.index.corpus.DocumentReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentIndexMerger extends GenericIndexMerger<Document> {

  public DocumentIndexMerger(TupleFlowParameters p) throws Exception {
    super(p);
  }

  @Override
  public boolean mappingKeys() {
    return false;
  }

  @Override
  public Processor<Document> createIndexWriter(TupleFlowParameters parameters) throws Exception {
    return new DocumentIndexWriter(parameters);
  }

  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {
    assert (keyIterators.size() == 1) : "Found two identical keys when merging names. Documents can never be combined.";
    Document d = ((DocumentReader.DocumentIterator) keyIterators.get(0).iterator).getDocument() ;
    this.writer.process(d);
  }
}
