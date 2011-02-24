package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.galagosearch.core.retrieval.structured.NavigableIterator;
import org.galagosearch.tupleflow.Utility;

/**
 * Base class for any data structures that map a key value
 * to a list of data, where one cannot assume the list can be
 * held in memory
 *
 *
 * @author irmarc
 */
public abstract class KeyListReader implements StructuredIndexPartReader {

  protected GenericIndexReader reader;

  public KeyListReader(String filename) throws FileNotFoundException, IOException {
    reader = GenericIndexReader.getIndexReader(filename);
  }

  public KeyListReader(GenericIndexReader r) {
    this.reader = r;
  }

  public void close() throws IOException {
    reader.close();
  }

  public abstract Iterator getIterator() throws IOException;

  public abstract ListIterator getListIterator() throws IOException;

  public abstract class ListIterator extends NavigableIterator implements ValueIterator {

    protected GenericIndexReader.Iterator source;
    protected byte[] key;
    protected long dataLength;

    public ListIterator(GenericIndexReader.Iterator it) throws IOException {
      // implementation of this should load data
      reset(it);
    }

    public long getByteLength() throws IOException {
      return dataLength;
    }

    public abstract void reset(GenericIndexReader.Iterator it) throws IOException;

    public String getKey() {
      return Utility.toString(key);
    }

    public byte[] getKeyBytes() {
      return key;
    }

    public boolean nextEntry() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public abstract class Iterator implements KeyIterator {

    protected GenericIndexReader.Iterator iterator;
    protected GenericIndexReader reader;

    public Iterator(GenericIndexReader reader) throws IOException {
      this.reader = reader;
      reset();
    }

    public boolean isDone() {
      return iterator.isDone();
    }

    public boolean skipToKey(byte[] key) throws IOException {
      iterator.skipTo(key);
      if (Utility.compare(key, iterator.getKey()) == 0) {
        return true;
      }
      return false;
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

    public byte[] getKeyBytes() {
      return iterator.getKey();
    }

    public String getKey() {
      return Utility.toString(iterator.getKey());
    }

    public abstract String getStringValue();
  }
}
