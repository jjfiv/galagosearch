// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.galagosearch.core.index.GenericIndexReader;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.index.KeyIterator;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.VByteInput;

/**
 *
 * @author trevor
 */
public class DocumentIndexReader extends DocumentReader {

  public DocumentIndexReader(String fileName) throws FileNotFoundException, IOException {
    super(fileName);
  }

  public DocumentIndexReader(GenericIndexReader reader) {
    super(reader);
  }

  public DocumentReader.DocumentIterator getIterator() throws IOException {
    return new Iterator(reader);
  }

  public Document getDocument(String key) throws IOException {
    return new Iterator(reader).getDocument();
  }

  public Map<String, NodeType> getNodeTypes() {
    return new HashMap();
  }

  public ValueIterator getIterator(Node node) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public class Iterator extends DocumentReader.DocumentIterator {

    Iterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    public Document getDocument() throws IOException {
      String key = Utility.toString(iterator.getKey());
      DataStream stream = iterator.getValueStream();
      return decodeDocument(key, stream);
    }

    Document decodeDocument(String key, DataStream stream) throws IOException {
      VByteInput input = new VByteInput(stream);
      Document document = new Document();

      // The first string is the document text, followed by
      // key/value metadata pairs.
      document.identifier = key;
      document.text = input.readString();
      document.metadata = new HashMap<String, String>();

      while (!stream.isDone()) {
        String mapKey = input.readString();
        String mapValue = input.readString();

        document.metadata.put(mapKey, mapValue);
      }

      return document;
    }

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
