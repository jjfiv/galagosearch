// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import org.galagosearch.core.index.corpus.DocumentReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.Utility.ByteArrComparator;

public class MemoryCorpus extends DocumentReader {

  private class MemDocIterator implements DocumentIterator {

    private Iterator<byte[]> keyIterator;
    private byte[] currKey;

    public MemDocIterator(Iterator<byte[]> iterator) throws IOException {
      this.keyIterator = iterator;
      nextDocument();
    }

    public void skipTo(byte[] key) throws IOException {
      keyIterator = corpusData.tailMap(key).keySet().iterator();
      nextDocument();
    }

    public String getKey() {
      return Utility.toString(currKey);
    }

    public byte[] getKeyBytes() {
      return currKey;
    }

    public boolean isDone() {
      return currKey == null;
    }

    public Document getDocument() throws IOException {
      return corpusData.get(currKey);
    }

    public boolean nextDocument() throws IOException {
      if (keyIterator.hasNext()) {
        currKey = keyIterator.next();
        return true;
      } else {
        currKey = null;
        return false;
      }
    }

    public int compareTo(DocumentIterator o) {
      return Utility.compare(this.getKeyBytes(), o.getKeyBytes());
    }
  }
  private TreeMap<byte[], Document> corpusData;

  public MemoryCorpus() {
    corpusData = new TreeMap( new ByteArrComparator() );
  }

  public void addDocument(Document doc) {
    // save a subset of the document.
    Document d = new Document();
    d.identifier = doc.identifier;
    d.text = doc.text;
    d.metadata = doc.metadata;
    corpusData.put( Utility.fromString(d.identifier), d);
  }

  @Override
  public void close() throws IOException {
    // clean up data.
    corpusData = null;
  }

  @Override
  public DocumentIterator getIterator() throws IOException {
    return new MemDocIterator(corpusData.keySet().iterator()) {
    };
  }

  @Override
  public Document getDocument(String key) throws IOException {
    return corpusData.get(Utility.fromString(key));
  }
}
