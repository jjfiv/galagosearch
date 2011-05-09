// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.IOException;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.util.IntArray;
import org.galagosearch.tupleflow.Utility;

public class MemoryDocumentLengths {
  
  private IntArray lengths = new IntArray(256);
  private int offset;
  
  public MemoryDocumentLengths(int offset){
    this.offset = offset;
  }
  
  public void addDocument(int docNum, int length){
    assert(offset + lengths.getPosition() == docNum);
    // otherwise we will have a problem; - or need to add zeros
    
    lengths.add(length);
  }

  public int getLength(int docNum){
    return lengths.getBuffer()[docNum - offset];
  }
  
  
  // Allow documentLengths data to be iterated over
  public Iterator getIterator(){
    return new Iterator();
  }
  
  public class Iterator extends NumberedDocumentDataIterator {
    int current = 0;
    
    public void reset() throws IOException {
      current = 0;
    }

    public String getRecordString() {
      int docNum = current + offset;
      return docNum + ", " + lengths.getBuffer()[current];
    }
    
    //schiu
    //key is docNum in byte array format to be consistent with other readers
    public boolean skipTo(byte[] key) throws IOException{
    	current = Utility.toInt(key) - offset;
    	return (current >=0 || current < lengths.getPosition());
    }

    public void skipTo(int key) throws IOException{
    	current = key - offset;
    }

    public boolean nextRecord() throws IOException {
      current++;
      if(current < lengths.getPosition())
        return true;
      return false;
    }

    public NumberedDocumentData getDocumentData() throws IOException {
      int docNum = current + offset;
      return new NumberedDocumentData("","",docNum,lengths.getBuffer()[current]);
    }

    public String getKey(){
      return Integer.toString(current + offset);
    }
    
    public byte[] getKeyBytes(){
    	return Utility.fromInt(offset+current);
    }
  }
}

