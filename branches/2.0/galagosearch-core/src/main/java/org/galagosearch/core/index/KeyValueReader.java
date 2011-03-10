package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.Utility;

/**
 * Superclass for any iterator that provides a direct mapping
 * of keys -> values, and we assume the entire value can be read into
 * memory.
 *
 * Implementations should provide their own methods for manipulating the keys and
 * values.
 * 
 * @author irmarc
 */
public abstract class KeyValueReader implements StructuredIndexPartReader {

  protected GenericIndexReader reader;

  public KeyValueReader(String filename) throws FileNotFoundException, IOException {
    reader = GenericIndexReader.getIndexReader(filename);
  }

  public KeyValueReader(GenericIndexReader r) {
    this.reader = r;
  }

  public void close() throws IOException {
    reader.close();
  }

  public abstract Iterator getIterator() throws IOException;

  public static abstract class Iterator implements KeyIterator {

    protected GenericIndexReader.Iterator iterator;
    protected GenericIndexReader reader;

    public Iterator(GenericIndexReader reader) throws IOException {
      this.reader = reader;
      reset();
    }

    public boolean isDone() {
      return iterator.isDone();
    }

    public boolean moveToKey(byte[] key) throws IOException {
      iterator.skipTo(key);
      if (Utility.compare(key, iterator.getKey()) == 0) {
        return true;
      }
      return false;
    }

    public int compareTo(KeyIterator other) {
      try {
        return Utility.compare(getKeyBytes(), other.getKeyBytes());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public boolean nextKey() throws IOException {
      return (iterator.nextKey());
    }

    public void reset() throws IOException {
      iterator = reader.getIterator();
    }

    public byte[] getValueBytes() throws IOException {
      return iterator.getValueBytes();
    }

    public DataStream getValueStream() throws IOException {
      return iterator.getValueStream();
    }

    public byte[] getKeyBytes() {
      return iterator.getKey();
    }

    public String getKey() {
      return Utility.toString(iterator.getKey());
    }

    public abstract String getValueString();
  }
}
