/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.mergeindex;

import java.io.IOException;
import java.util.PriorityQueue;
import org.galagosearch.core.index.corpus.DocumentReader;
import org.galagosearch.core.index.corpus.DocumentReader.DocumentIterator;
import org.galagosearch.core.parse.Document;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass( className="org.galagosearch.core.index.corpus.DocumentReader" )
@OutputClass( className="org.galagosearch.core.parse.Document" )
public class DocumentReaderMerger extends StandardStep<DocumentReader, Document> {

  PriorityQueue<DocumentIterator> queue = new PriorityQueue();

  @Override
  public void process(DocumentReader i) throws IOException {
    queue.offer(i.getIterator());
  }

  @Override
  public void close() throws IOException {
    while (queue.size() > 1) {
      DocumentIterator head = queue.poll();
      DocumentIterator next = queue.peek();

      do {
        processor.process(head.getDocument());
        head.nextDocument();
      } while ((!head.isDone()) && (head.compareTo(next) <= 0));

      if (!head.isDone()) {
        queue.offer(head);
      }
    }

    if (queue.size() == 1) {
      // process everything from the final wrapper
      DocumentIterator head = queue.poll();
      do {
        processor.process( head.getDocument() );
      } while (head.nextDocument());
    }

    processor.close();
  }
}
