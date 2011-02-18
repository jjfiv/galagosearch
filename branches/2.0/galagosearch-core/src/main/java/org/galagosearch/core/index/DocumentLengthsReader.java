// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import org.galagosearch.core.retrieval.structured.NumberedDocumentDataIterator;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.core.util.CallTable;
import org.galagosearch.tupleflow.Utility;

/**
 * Reads documents lengths from a document lengths file.
 * Iterator provides a useful interface for dumping the contents of the file.
 *
 * offset is the first document number (for sequential sharding purposes)
 * 
 * @author trevor, sjh
 */
public class DocumentLengthsReader extends KeyDataReader {

  public DocumentLengthsReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
  }

  public DocumentLengthsReader(GenericIndexReader r) {
    super(r);
  }

  public int getLength(int document) throws IOException {
    return Utility.uncompressInt(reader.getValueBytes(Utility.fromInt(document)), 0);
  }

  public Iterator getIterator() throws IOException {
    return new Iterator(reader);
  }

  public class Iterator extends KeyDataReader.Iterator {

    public Iterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    public String getRecordString() {
      try {
        StringBuilder sb = new StringBuilder();
        sb.append(Utility.toInt(iterator.getKey())).append(",");
        sb.append(Utility.uncompressInt(iterator.getValueBytes(), 0));
        return sb.toString();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public void skipTo(int key) throws IOException {
      byte[] bkey = Utility.fromInt(key);
      iterator.skipTo(bkey);
    }

    public NumberedDocumentData getDocumentData() throws IOException {
      int docNum = Utility.toInt(iterator.getKey());
      int length = Utility.uncompressInt(iterator.getValueBytes(), 0);
      return new NumberedDocumentData("", "", docNum, length);
    }

    public int getCurrentDocument() {
      return Utility.toInt(iterator.getKey());
    }
  }
}
