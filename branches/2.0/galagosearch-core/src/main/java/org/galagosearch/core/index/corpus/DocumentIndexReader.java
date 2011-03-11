// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.index.GenericIndexReader;
import org.galagosearch.core.index.KeyIterator;
import org.galagosearch.core.index.KeyToListIterator;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.DataIterator;
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
    Iterator i = new Iterator(reader);
    byte[] k = Utility.fromString(key);
    i.moveToKey(k);
    assert (Utility.compare(i.getKeyBytes(), k) == 0);
    return i.getDocument();
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("content", new NodeType(Iterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("content")) {
      return new ValueIterator(new Iterator(reader));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
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
      return new ValueIterator(this);
    }
  }

  public class ValueIterator extends KeyToListIterator implements DataIterator<Document> {

    public ValueIterator(KeyIterator ki) {
      super(ki);
    }

    public String getEntry() throws IOException {
      return ((Iterator) iterator).getDocument().toString();
    }

    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document getData() {
      try {
        return ((Iterator) iterator).getDocument();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
}
