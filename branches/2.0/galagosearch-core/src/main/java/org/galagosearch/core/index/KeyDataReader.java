package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
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
public abstract class KeyDataReader {

  protected GenericIndexReader reader;

  public KeyDataReader(String filename) throws FileNotFoundException, IOException {
    reader = GenericIndexReader.getIndexReader(filename);
  }

  public KeyDataReader(GenericIndexReader r) {
    this.reader = r;
  }

  public void close() throws IOException {
    reader.close();
  }

  public abstract Iterator getIterator() throws IOException;

  public abstract class Iterator {

    protected GenericIndexReader.Iterator iterator;
    protected GenericIndexReader reader;

    public Iterator(GenericIndexReader reader) throws IOException {
      this.reader = reader;
      reset();
    }

    public boolean skipTo(byte[] key) throws IOException {
      iterator.skipTo(key);
      if (Utility.compare(key, iterator.getKey()) == 0) {
        return true;
      }
      return false;
    }

    public boolean nextRecord() throws IOException {
      return (iterator.nextKey());
    }

    public void reset() throws IOException {
      iterator = reader.getIterator();
    }

    public byte[] getValueBytes() throws IOException {
      return iterator.getValueBytes();
    }

    public byte[] getKeyBytes() {
      return iterator.getKey();
    }

    public String getKey() {
      return Utility.toString(iterator.getKey());
    }

    public abstract String getRecordString();
  }
}
