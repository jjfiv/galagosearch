package org.galagosearch.core.mergeindex.parallel;

import java.io.IOException;
import java.util.PriorityQueue;

import org.galagosearch.core.retrieval.structured.ExtentIndexIterator;
import org.galagosearch.core.mergeindex.KeyExtent;
import org.galagosearch.core.retrieval.structured.Extent;
import org.galagosearch.core.types.DocumentSplit;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Processor;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.Verified;

@Verified
@InputClass(className = "org.galagosearch.core.retrieval.structured.ExtentIndexIterator")
@OutputClass(className = "org.galagosearch.core.mergeindex.KeyExtent")
public class ExtentIteratorMerger extends StandardStep<ExtentIndexIterator, KeyExtent> {

  NumberMappingReader mapping;
  PriorityQueue<ExtentIndexIteratorWrapper> queue = new PriorityQueue();

  public ExtentIteratorMerger(TupleFlowParameters p) throws IOException {
    String stream = p.getXML().get("stream");
    TypeReader<DocumentSplit> streamReader = p.getTypeReader(stream);
    DocumentSplit mapFolder = streamReader.read();
    mapping = new NumberMappingReader(mapFolder.fileName);
  }

  public ExtentIteratorMerger(Processor<KeyExtent> processor) throws IncompatibleProcessorException{
    super();
    this.setProcessor( processor );
  }
  
  public void process(ExtentIndexIterator ei) throws IOException {
    queue.add(new ExtentIndexIteratorWrapper(ei));
  }


  long counter = 0;

  public void close() throws IOException {
    
    while(queue.size() > 1){
      ExtentIndexIteratorWrapper head = queue.poll();
      
      for(Extent e : head.documentExtents.toArray()){
        KeyExtent ke = new KeyExtent( head.key , head.document, e );
        processor.process( ke );
        counter++;
      }
      
      if(head.next())
        queue.offer(head);
    }
    
    ExtentIndexIteratorWrapper head = queue.poll();
    do{
      for(Extent e : head.documentExtents.toArray()){
        KeyExtent ke = new KeyExtent( head.key , head.document, e );
        processor.process( ke );
        counter++;
      }
    } while(head.next());

    processor.close();
  }
  
  private class ExtentIndexIteratorWrapper implements Comparable<ExtentIndexIteratorWrapper> {
    ExtentIndexIterator iterator;
    String key;
    int document;
    ExtentArray documentExtents;
    
    public ExtentIndexIteratorWrapper(ExtentIndexIterator iterator) throws IOException{
      this.iterator = iterator;
      this.documentExtents = iterator.extents();
      this.key = iterator.getKey();
      this.document = mapping.getNewDocNumber(iterator.indexId, documentExtents.getBuffer()[0].document);
    }
    
    public boolean next() throws IOException{
      if(iterator.nextRecord() ){
        this.documentExtents = iterator.extents();
        this.key = iterator.getKey();
        this.document = mapping.getNewDocNumber(iterator.indexId, documentExtents.getBuffer()[0].document);
        return true;
      }
      return false;
    }

    public int compareTo(ExtentIndexIteratorWrapper other) {

      if(! this.key.equals(other.key)){
        return Utility.compare(this.key, other.key);
      } else {
        return Utility.compare(this.document,
            other.document);
      }
    }
  }
}


