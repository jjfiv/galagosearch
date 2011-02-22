package org.galagosearch.core.mergeindex.sequential;

import java.io.IOException;
import java.util.PriorityQueue;

import org.galagosearch.core.retrieval.structured.ExtentIndexIterator;
import org.galagosearch.core.mergeindex.KeyExtent;
import org.galagosearch.core.retrieval.structured.Extent;
import org.galagosearch.core.index.KeyIterator;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

@Verified
@InputClass(className = "org.galagosearch.core.mergeindex.ExtentIndexIterator")
@OutputClass(className = "org.galagosearch.core.mergeindex.KeyExtent")
public class ExtentIteratorMerger extends StandardStep<ExtentIndexIterator, KeyExtent> {

  PriorityQueue<ExtentIndexIteratorWrapper> queue = new PriorityQueue();

  public ExtentIteratorMerger(TupleFlowParameters p) {
    // Nothing needs to be done
  }

  public ExtentIteratorMerger(Processor<KeyExtent> processor) throws IncompatibleProcessorException {
    super();
    this.setProcessor(processor);
  }

  public void process(ExtentIndexIterator ei) throws IOException {
    queue.add(new ExtentIndexIteratorWrapper(ei));
  }
  long counter = 0;

  public void close() throws IOException {


    while (queue.size() > 1) {
      ExtentIndexIteratorWrapper head = queue.poll();
      ExtentIndexIteratorWrapper next = queue.peek();

      do{
        for (Extent e : head.documentExtents.toArray()) {
          KeyExtent ke = new KeyExtent(head.key, head.document, e);
          processor.process(ke);
          counter++;
        }
        head.next();
      } while((! head.isDone) && (head.compareTo(next) <= 0));

      if( ! head.isDone) {
        queue.offer(head);
      }
    }

    // process everything from the final wrapper
    ExtentIndexIteratorWrapper head = queue.poll();
    do {
      for (Extent e : head.documentExtents.toArray()) {
        KeyExtent ke = new KeyExtent(head.key, head.document, e);
        processor.process(ke);
        counter++;
      }
    } while (head.next());

    processor.close();
  }

  private class ExtentIndexIteratorWrapper implements Comparable<ExtentIndexIteratorWrapper> {
    boolean isDone;
    ExtentIndexIterator iterator;
    byte[] key;
    int document;
    ExtentArray documentExtents;

    public ExtentIndexIteratorWrapper(ExtentIndexIterator iterator) throws IOException {
      this.iterator = iterator;
      this.documentExtents = iterator.extents();
      this.key = iterator.getKeyBytes();
      this.document = documentExtents.getBuffer()[0].document;
      isDone = false;
    }

    public boolean next() throws IOException {
      if (isDone) {
        return false;
      }

      if (iterator.nextRecord()) {
        this.documentExtents = iterator.extents();
        this.key = iterator.getKeyBytes();
        this.document = documentExtents.getBuffer()[0].document;
        return true;
      }

      isDone = true;
      return false;
    }

    public int compareTo(ExtentIndexIteratorWrapper other) {
      int result;
      result = Utility.compare(this.key, other.key);
      if (result != 0) {
        return result;
      }
      return Utility.compare(this.document, other.document);
    }
  }
}
