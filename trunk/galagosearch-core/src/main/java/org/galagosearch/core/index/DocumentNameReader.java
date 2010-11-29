// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.IOException;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;

import org.galagosearch.core.types.DataMapItem;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Utility;

/**
 * Reads a binary file of document names produced by DocumentNameWriter2.
 * BulkTrees are used 
 * 
 * 
 * Reverse lookup is provided 
 * 
 * @author sjh
 */
public class DocumentNameReader {

  DataMapReader inputFl;
  DataMapReader inputRl;

  /** Creates a new instance of DocumentNameReader */
  public DocumentNameReader(String folder) throws IOException {
    inputFl = new DataMapReader(folder, "fl", true);
    inputRl = new DataMapReader(folder, "rl", false);
  }

  // gets the document name of the internal id index.
  public String get(int index) throws IOException {
    DataMapItem dmi = inputFl.get(Utility.fromInt(index));
    if (dmi == null) {
      throw new IOException("Unknown Document Number " + index);
    }
    return Utility.toString(dmi.value);
  }

  // gets the document id for some document name
  public int getDocumentId(String documentName) throws IOException {
    byte[] value = new byte[0];
    value = Utility.fromString(documentName);
    DataMapItem dmi = inputRl.get(value);
    if (dmi == null) {
      throw new IOException("Unknown Document Name " + documentName);
    }

    return Utility.toInt(dmi.value);
  }

  
  public NumberedDocumentDataIterator getNumberOrderIterator() throws IOException {
    return new Iterator(inputFl, true);
  }
  public NumberedDocumentDataIterator getNameOrderIterator() throws IOException {
    return new Iterator(inputRl, false);
  }

  public class Iterator extends NumberedDocumentDataIterator{
    
    boolean forwardLookup;
    DataMapReader input;
    DataMapReader.Iterator iterator;
    DataMapItem current;

    public Iterator(DataMapReader input, boolean forwardLookup) throws IOException {
      this.forwardLookup = forwardLookup;
      this.input = input;
      reset();
    }

    public void reset() throws IOException {
      iterator = input.getIterator();
      current = iterator.getItem();
    }

    public String getRecordString() {
      if(forwardLookup){
        return Utility.toInt(current.key) + ", " + Utility.toString(current.value);
      } else {
        return Utility.toInt(current.value) + ", " + Utility.toString(current.key);
      }      
    }

    public boolean nextRecord() throws IOException {
      if (iterator.nextRecord()) {
        current = iterator.getItem();
        return true;
      }
      return false;
    }

    public NumberedDocumentData getDocumentData() throws IOException {
      if(forwardLookup){
        return new NumberedDocumentData(Utility.toString(current.value), "", Utility.toInt(current.key), 0);
      } else {
        return new NumberedDocumentData(Utility.toString(current.key), "", Utility.toInt(current.value), 0);
      }      
    }

    public String getKey() {
      if(forwardLookup){
        return Integer.toString(Utility.toInt(current.key));
      } else {
        return Utility.toString(current.key);
      }
    }
  }
}
