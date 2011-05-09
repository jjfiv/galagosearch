// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.mergeindex.parallel;

import java.io.IOException;
import java.util.PriorityQueue;

import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

@Verified
@InputClass(className = "org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator")
@OutputClass(className = "org.galagosearch.core.types.NumberedDocumentData", order={"+number"})
public class NumberedDocumentDataIteratorMerger extends
    StandardStep<NumberedDocumentDataIterator, NumberedDocumentData> {

  NumberMappingReader mapping;
  PriorityQueue<NDDIWrapper> queue = new PriorityQueue();

  public NumberedDocumentDataIteratorMerger(TupleFlowParameters p) throws IOException{
    String stream = p.getXML().get("stream");
    TypeReader<DocumentSplit> streamReader = p.getTypeReader(stream);
    DocumentSplit mapFolder = streamReader.read();
    mapping = new NumberMappingReader(mapFolder.fileName);
  }

  public void process(NumberedDocumentDataIterator nddi) throws IOException {
    queue.add(new NDDIWrapper(nddi));
  }

  public void close() throws IOException {

    while(queue.size() > 1){
      NDDIWrapper head = queue.poll();

      processor.process(head.ndd);

      if(head.next())
        queue.offer(head);
    }
    
    NDDIWrapper head = queue.poll();
    do{
      processor.process(head.ndd);
    } while(head.next());

    processor.close();
  }
  
  private class NDDIWrapper implements Comparable<NDDIWrapper> {
    NumberedDocumentDataIterator iterator;
    NumberedDocumentData ndd;
    
    public NDDIWrapper(NumberedDocumentDataIterator iterator) throws IOException{
      this.iterator = iterator;
      this.ndd = iterator.getDocumentData();
      int newDocId = mapping.getNewDocNumber(iterator.indexId, ndd.number);
      ndd.number = newDocId;
    }
    
    public boolean next() throws IOException{
      if(iterator.nextRecord()){
        ndd = iterator.getDocumentData();
        int newDocId = mapping.getNewDocNumber(iterator.indexId, ndd.number);
        ndd.number = newDocId;

        return true;
      }
      return false;
    }

    public int compareTo(NDDIWrapper other) {
      return Utility.compare(ndd.number, other.ndd.number);
    }
  }
}
