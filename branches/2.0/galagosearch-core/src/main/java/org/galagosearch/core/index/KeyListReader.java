package org.galagosearch.core.index;

/**
 * Base class for any data structures that map a key value
 * to a list of data, where one cannot assume the list can be
 * held in memory
 *
 *
 * @author irmarc
 */
public class KeyListReader {
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
}
