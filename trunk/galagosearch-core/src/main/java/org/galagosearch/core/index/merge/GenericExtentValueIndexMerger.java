/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index.merge;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import org.galagosearch.core.retrieval.structured.ExtentValueIterator;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class GenericExtentValueIndexMerger<S> extends GenericIndexMerger<S> {

  // wrapper class for ExtentValueIterators

  private class ExtentValueIteratorWrapper implements Comparable<ExtentValueIteratorWrapper>{
    int indexId;
    ExtentValueIterator iterator;
    int currentDocument;
    ExtentArray currentExtentArray;
    DocumentMappingReader mapping;

    private ExtentValueIteratorWrapper(int indexId, ExtentValueIterator extentIterator, DocumentMappingReader mapping) {
      this.indexId = indexId;
      this.iterator = extentIterator;
      this.mapping = mapping;

      // initialization
      load();
    }

    public boolean next() throws IOException{
      boolean success = iterator.next();
      if(success){
        load();
      }
      return success;
    }

    // changes the document numbers in the extent array
    private void load() {
      this.currentExtentArray = iterator.extents();
      this.currentDocument = mapping.map(indexId, currentExtentArray.getBuffer()[0].document);
      for(int i=0 ; i < currentExtentArray.getPositionCount() ; i++){
        currentExtentArray.getBuffer()[i].document = this.currentDocument;
      }
    }


    public boolean isDone(){
      return iterator.isDone();
    }

    public int compareTo(ExtentValueIteratorWrapper other) {
      return Utility.compare(currentDocument, other.currentDocument);
    }
  }

  // overridden functions
  public GenericExtentValueIndexMerger(TupleFlowParameters parameters) throws Exception {
    super(parameters);
  }

  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException{
    PriorityQueue<ExtentValueIteratorWrapper> extentQueue = new PriorityQueue();
    for (KeyIteratorWrapper w : keyIterators) {
      ExtentValueIterator extentIterator = (ExtentValueIterator) w.iterator.getValueIterator();
      extentQueue.add( new ExtentValueIteratorWrapper( this.partIds.get( w ), extentIterator, this.mappingReader ) );
    }

    while( ! extentQueue.isEmpty() ){
      ExtentValueIteratorWrapper head = extentQueue.poll();
      transformExtentArray(key, head.currentExtentArray);
      if(head.next()){
        extentQueue.offer(head);
      }
    }
  }

  public abstract void transformExtentArray(byte[] key, ExtentArray extentArray) throws IOException ;
}
