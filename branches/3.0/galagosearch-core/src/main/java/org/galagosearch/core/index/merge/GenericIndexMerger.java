// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.merge;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import org.galagosearch.core.index.KeyIterator;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;

/**
 *
 * @author sjh
 */
public abstract class GenericIndexMerger<T> {

  // document mapping data
  protected boolean mappingKeys;
  protected DocumentMappingReader mappingReader = null;
  // input readers
  protected PriorityQueue<KeyIteratorWrapper> queue;
  protected HashMap<KeyIteratorWrapper, Integer> partIds;
  // output writer
  protected Processor<T> writer = null;

  public GenericIndexMerger(TupleFlowParameters parameters) throws Exception {
    String outputClass = (parameters.getXML().get("writerClass"));

    queue = new PriorityQueue();
    partIds = new HashMap();

    writer = createIndexWriter(parameters);
  }

  public void setDocumentMapping(DocumentMappingReader mappingReader) {
    this.mappingReader = mappingReader;
  }

  // this requires that the mappingReader has been set.
  public void setInputs(HashMap<StructuredIndexPartReader, Integer> readers) throws IOException {
    for (StructuredIndexPartReader r : readers.keySet()) {
      KeyIterator iterator = r.getIterator();
      if (iterator != null) {
        KeyIteratorWrapper w = new KeyIteratorWrapper(readers.get(r), iterator, mappingKeys, mappingReader);
        queue.offer(w);
        partIds.put(w, readers.get(r));
      }
    }
  }

  public void performKeyMerge() throws IOException {
    ArrayList<KeyIteratorWrapper> head = new ArrayList();
    while (!queue.isEmpty()) {
      head.clear();
      head.add(queue.poll());
      while ((!queue.isEmpty())
              && (queue.peek().compareTo(head.get(0)) == 0)) {
        head.add(queue.poll());
      }
      byte[] key = head.get(0).getKeyBytes();

      performValueMerge(key, head);

      for (KeyIteratorWrapper i : head) {
        if ( i.nextKey() ) {
          queue.offer(i);
        }
      }
    }
  }

  public void close() throws IOException {
    writer.close();
  }

  // creates the writer object - needs to be implemented for each merger
  public abstract Processor<T> createIndexWriter(TupleFlowParameters parameters) throws Exception;

  // merges the of values for the current key
  public abstract void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException;
}
