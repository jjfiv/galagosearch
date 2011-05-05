// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.merge;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.TupleFlowParameters;

/**
 *
 * @author sjh
 */
public abstract class GenericIndexMerger<T> {

  protected boolean mappingKeys;
  protected DocumentMappingReader mappingReader = null;
  protected PriorityQueue<KeyIteratorWrapper> queue;
  protected HashMap<KeyIteratorWrapper, Integer> partIds;
  protected Processor<T> output;

  public GenericIndexMerger(TupleFlowParameters parameters) throws Exception {
    String outputClass = (parameters.getXML().get("writerClass"));
    System.err.println( outputClass );
    Class clazz = Class.forName(outputClass);
    System.err.println( clazz );
    Constructor c = clazz.getConstructor(TupleFlowParameters.class);
    System.err.println( c );
    output = (Processor<T>) c.newInstance(parameters);

    queue = new PriorityQueue();
    partIds = new HashMap();
  }

  public void setDocumentMapping(DocumentMappingReader mappingReader) {
    this.mappingReader = mappingReader;
  }

  // this requires that the mappingReader has been set.
  public void setInputs(HashMap<StructuredIndexPartReader, Integer> readers) throws IOException {
    for (StructuredIndexPartReader r : readers.keySet()) {
      KeyIteratorWrapper w = new KeyIteratorWrapper(readers.get(r), r.getIterator(), mappingKeys, mappingReader);
      queue.offer(w);
      partIds.put(w, readers.get(r));
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
        if (i.nextKey()) {
          queue.offer(i);
        }
      }
    }
  }

  public abstract void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException;

  public void close() throws IOException {
    output.close();
  }
}
