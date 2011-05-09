// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.IOException;

import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.util.ObjectArray;
import org.galagosearch.tupleflow.Utility;

public class MemoryDocumentNames {

  private ObjectArray<String> names = new ObjectArray(String.class,256);
  private int offset;

  public MemoryDocumentNames(int offset){
    this.offset = offset;
  }

  public void addDocument(int docNum, String name){
    assert(names.getPosition() + offset == docNum);
    names.add(name);
  }

  public String getDocumentName(int docNum){
    return names.getBuffer()[docNum - offset];
  }

  int getDocumentId(String document) {
    throw new UnsupportedOperationException("Not yet implemented");
  }


  // Allow documentNames data to be iterated over 
  // (in document number order)
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
      return docNum + ", " + names.getBuffer()[current];
    }

    public boolean nextRecord() throws IOException {
      current++;
      if(current < names.getPosition())
        return true;
      return false;
    }
    
    //schiu
    //key is docNum in byte array format to be consistent with other readers
    public boolean skipTo(byte[] key) throws IOException{
    	current = Utility.toInt(key) - offset;
    	return (current >=0 || current < names.getPosition());
    }

    public void skipTo(int key) throws IOException{
    	current = key - offset;
    }
    
    public NumberedDocumentData getDocumentData() throws IOException {
      int docNum = current + offset;
      return new NumberedDocumentData( names.getBuffer()[current],"",docNum,-1);
    }

    public String getKey(){
      return Integer.toString(current + offset);
    }
    
    public byte[] getKeyBytes(){
    	return Utility.fromInt(offset+current);
    }
  }
}

