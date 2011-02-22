package org.galagosearch.core.mergeindex.sequential;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

@Verified
@InputClass(className = "org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator")
@OutputClass(className = "org.galagosearch.core.types.NumberedDocumentData", order={"+number"})
public class NumberedDocumentDataIteratorMerger extends
    StandardStep<NumberedDocumentDataIterator, NumberedDocumentData> {
  
  PriorityQueue<NDDIWrapper> queue = new PriorityQueue();
  
  public void process(NumberedDocumentDataIterator nddi) throws IOException {
    queue.add(new NDDIWrapper(nddi));
  }

  public void close() throws IOException {


    while(queue.size() > 1){
      NDDIWrapper head = queue.poll();
      NDDIWrapper next = queue.peek();

      do{
        processor.process(head.ndd);
        head.next();
      } while((!head.isDone) && head.compareTo(next) <= 0);

      if(!head.isDone)
        queue.offer(head);
    }

    // process the final wrapper
    NDDIWrapper head = queue.poll();
    do{
      processor.process(head.ndd);
    } while(head.next());

    processor.close();
  }
  
  private class NDDIWrapper implements Comparable<NDDIWrapper> {
    NumberedDocumentDataIterator iterator;
    NumberedDocumentData ndd;
    boolean isDone;

    
    public NDDIWrapper(NumberedDocumentDataIterator iterator) throws IOException{
      this.iterator = iterator;
      this.ndd = iterator.getDocumentData();
      isDone = false;
    }
    
    public boolean next() throws IOException{
      if(isDone)
        return false;

      if(iterator.nextKey()){
        ndd = iterator.getDocumentData();
        return true;
      }

      isDone = true;
      return false;
    }

    public int compareTo(NDDIWrapper other) {
      return Utility.compare(ndd.number, other.ndd.number);
    }
  }
}
