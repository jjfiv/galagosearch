// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.zip.GZIPInputStream;
import org.galagosearch.core.parse.Document;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * Reader for corpus folders
 *  - corpus folder is a parallel index structure:
 *  - one key.index file
 *  - several data files (0 -> n)
 *
 *
 * @author sjh
 */
public class CorpusReader extends DocumentReader {

  SplitIndexReader indexReader;
  boolean compressed;

  public CorpusReader(String fileName) throws FileNotFoundException, IOException {
    indexReader = new SplitIndexReader(fileName);
    compressed = indexReader.getManifest().get("compressed", true);
  }

  public CorpusReader() {
    // do nothing -- shouldn't really be used by anyone ever
    throw new UnsupportedOperationException("NO!");
  }

  public void close() throws IOException {
    indexReader.close();
  }

  public DocumentReader.DocumentIterator getIterator() throws IOException {
    return new Iterator(indexReader.getIterator());
  }

  public Document getDocument(String key) throws IOException {
    SplitIndexReader.Iterator iterator = indexReader.getIterator(Utility.fromString(key));
    if (iterator == null) {
      return null;
    }
    return new Iterator(iterator).getDocument();
  }

  public class Iterator implements DocumentReader.DocumentIterator {

    SplitIndexReader.Iterator iterator;
    Document document;

    Iterator(SplitIndexReader.Iterator iterator) throws IOException {
      this.iterator = iterator;
    }

    public void skipTo(byte[] key) throws IOException {
      iterator.skipTo(key);
      document = null;
    }

    public String getKey() {
      return Utility.toString(iterator.getKey());
    }

    public byte[] getKeyBytes() {
      return iterator.getKey();
    }

    public boolean isDone() {
      return iterator.isDone();
    }

    public Document getDocument() throws IOException {
      // only decode once
      if (document == null) {
        document = decodeDocument(iterator.getValueBytes());
      }
      return document;
    }

    public boolean nextDocument() throws IOException {
      document = null;
      return iterator.nextKey();
    }

    private Document decodeDocument(byte[] docData) throws IOException {

      ByteArrayInputStream stream = new ByteArrayInputStream(docData);
      ObjectInputStream docInput;
      if (compressed) {
        docInput = new ObjectInputStream(new GZIPInputStream(stream));
      } else {
        docInput = new ObjectInputStream(stream);
      }

      Document document = null;
      try {
        document = (Document) docInput.readObject();
      } catch (ClassNotFoundException ex) {
        throw new IOException("Expected to find a serialized document here, " + "but found something else instead.", ex);
      }

      docInput.close();

      return document;
    }

    public int compareTo(DocumentIterator o) {
      return Utility.compare(this.getKeyBytes(), o.getKeyBytes());
    }
  }
}
