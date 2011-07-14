// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.galagosearch.core.index.GenericIndexReader;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
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

  boolean compressed;

  public CorpusReader(String fileName) throws FileNotFoundException, IOException {
    super(fileName);
    compressed = reader.getManifest().get("compressed", true);
  }

  public CorpusReader(GenericIndexReader r) {
    super(r);
    compressed = reader.getManifest().get("compressed", true);
  }
  
  public DocumentReader.DocumentIterator getIterator() throws IOException {
    return new Iterator(reader);
  }

  public Document getDocument(String key) throws IOException {
    Iterator i = new Iterator(reader);
    byte[] k = Utility.fromString(key);
    if (i.findKey(k)) {
      return i.getDocument();
    } else {
      return null;
    }
  }

  public Map<String, NodeType> getNodeTypes() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public ValueIterator getIterator(Node node) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public class Iterator extends DocumentReader.DocumentIterator {

    Iterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    public Document getDocument() throws IOException {
      return decodeDocument(iterator.getValueBytes());
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

    @Override
    public String getValueString() {
      try {
        return getDocument().toString();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public ValueIterator getValueIterator() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
