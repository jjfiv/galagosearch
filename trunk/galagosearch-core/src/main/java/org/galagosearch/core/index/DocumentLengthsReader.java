// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Utility;

/**
 * Reads documents lengths from a document lengths file.
 * Iterator provides a useful interface for dumping the contents of the file.
 *
 * offset is the first document number (for sequential sharding purposes)
 * 
 * @author trevor, sjh
 */
public class DocumentLengthsReader {

  IndexReader reader;

  public DocumentLengthsReader(String filename) throws FileNotFoundException, IOException {

    reader = new IndexReader(filename);
  }

  public void close() throws IOException {
    reader.close();
  }

  public int getLength(int document) throws IOException {
    return Utility.uncompressInt(reader.getValueBytes(Utility.fromInt(document)), 0);
  }

  public NumberedDocumentDataIterator getIterator() throws IOException {
    return new Iterator(reader);
  }

  /*
   * Iterator uses the buffer above
   * Perhaps it would be faster to use a new data stream...
   *
   */
  public class Iterator extends NumberedDocumentDataIterator {

    IndexReader.Iterator iterator;
    IndexReader reader;

    public Iterator(IndexReader reader) throws IOException {
      this.reader = reader;
      reset();
    }

    public void reset() throws IOException {
      iterator = reader.getIterator();
    }

    public String getRecordString() {
      try {
        StringBuilder sb = new StringBuilder();
        sb.append(Utility.toInt(iterator.getKey())).append(",");
        sb.append(Utility.uncompressInt(iterator.getValueBytes(),0));
        return sb.toString();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public boolean nextRecord() throws IOException {
      return (iterator.nextKey());
    }

    public void skipTo( int key ) throws IOException{
      byte[] bkey = Utility.fromInt(key);
      while( Utility.compare( bkey, iterator.getKey()) != 0 ){
        nextRecord();
      }
      // too slow:
      // iterator.skipTo(key);
    }

    public NumberedDocumentData getDocumentData() throws IOException {
      int docNum = Utility.toInt(iterator.getKey());
      int length = Utility.uncompressInt(iterator.getValueBytes(), 0);
      return new NumberedDocumentData("", "", docNum, length);
    }

    public String getKey() {
      return Utility.toString(iterator.getKey());
    }
  }
}
