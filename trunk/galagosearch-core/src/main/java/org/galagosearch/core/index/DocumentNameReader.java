// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.IOException;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.KeyValuePair;

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

  IndexReader flIndex;
  IndexReader rlIndex;

  /** Creates a new instance of DocumentNameReader */
  public DocumentNameReader(String fileName) throws IOException {
    // Ensure that we are dealing with the correct fileName
    if (fileName.endsWith(".fl") || fileName.endsWith(".rl")) {
      fileName = fileName.substring(0, fileName.lastIndexOf("."));
    }

    flIndex = new IndexReader(fileName + ".fl");
    rlIndex = new IndexReader(fileName + ".rl");
  }

  // gets the document name of the internal id index.
  public String get(int index) throws IOException {
    byte[] data = flIndex.getValueBytes(Utility.fromInt(index));

    if (data == null) {
      throw new IOException("Unknown Document Number : " + index);
    }
    return Utility.toString(data);
  }

  // gets the document id for some document name
  public int getDocumentId(String documentName) throws IOException {
    byte[] data = rlIndex.getValueBytes(Utility.fromString(documentName));

    if (data == null) {
      throw new IOException("Unknown Document Name : " + documentName);
    }
    return Utility.toInt(data);
  }

  public NumberedDocumentDataIterator getNumberOrderIterator() throws IOException {
    return new Iterator(flIndex, true);
  }

  public NumberedDocumentDataIterator getNameOrderIterator() throws IOException {
    return new Iterator(rlIndex, false);
  }

  public class Iterator extends NumberedDocumentDataIterator {

    boolean forwardLookup;
    IndexReader input;
    IndexReader.Iterator iterator;
    KeyValuePair current;

    public Iterator(IndexReader input, boolean forwardLookup) throws IOException {
      this.forwardLookup = forwardLookup;
      this.input = input;
      reset();
    }

    public void reset() throws IOException {
      iterator = input.getIterator();

      byte[] key = iterator.getKey();
      byte[] value = iterator.getValueBytes();
      current = new KeyValuePair(key, value);
    }

    public String getRecordString() {
      if (forwardLookup) {
        return Utility.toInt(current.key) + ", " + Utility.toString(current.value);
      } else {
        return Utility.toInt(current.value) + ", " + Utility.toString(current.key);
      }
    }

    public boolean nextRecord() throws IOException {
      iterator.nextKey();

      if (iterator.isDone()) {
        return false;
      }

      byte[] key = iterator.getKey();
      byte[] value = iterator.getValueBytes();
      current = new KeyValuePair(key, value);

      return true;
    }

    public NumberedDocumentData getDocumentData() throws IOException {
      if (forwardLookup) {
        return new NumberedDocumentData(Utility.toString(current.value), "", Utility.toInt(current.key), 0);
      } else {
        return new NumberedDocumentData(Utility.toString(current.key), "", Utility.toInt(current.value), 0);
      }
    }

    public String getKey() {
      if (forwardLookup) {
        return Integer.toString(Utility.toInt(current.key));
      } else {
        return Utility.toString(current.key);
      }
    }

    public byte[] getKeyBytes() {
      return current.key;
    }

    public void skipTo(int key) throws IOException {
      iterator.skipTo(Utility.fromInt(key));
      byte[] newkey = iterator.getKey();
      byte[] newvalue = iterator.getValueBytes();
      current = new KeyValuePair(newkey, newvalue);
    }
  }
}
