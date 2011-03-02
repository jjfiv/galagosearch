// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.util.Map;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.DataIterator;
import org.galagosearch.tupleflow.VByteInput;

/**
 * Refactored in Galago 2.0 to fit in as part of the KeyValueReader paradigm.
 *
 *
 * @author irmarc
 */
public class DocumentContentReader extends KeyValueReader {

  public DocumentContentReader(String fileName) throws FileNotFoundException, IOException {
    this(GenericIndexReader.getIndexReader(fileName));
  }

  public DocumentContentReader(GenericIndexReader reader) {
    super(reader);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("content", new NodeType(Iterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("content")) {
      return new ValueIterator(new KeyIterator(reader));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  // This is nice because it won't decode data unless asked to, making it
  // efficient for just iterating over metadata.
  public class KeyIterator extends KeyValueReader.Iterator {

    Map<String, String> currentMetadata;
    String currentContent;
    VByteInput currentStream;

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
      currentContent = null;
      currentMetadata = null;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      currentContent = null;
      currentMetadata = null;
    }

    @Override
    public boolean nextKey() throws IOException {
      currentMetadata = null;
      currentContent = null;
      return (super.nextKey());
    }

    @Override
    public boolean moveToKey(byte[] key) throws IOException {
      currentMetadata = null;
      currentContent = null;
      return (super.moveToKey(key));
    }

    @Override
    public String getValueString() {
      try {
        return (getDocument().toString());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public Map<String, String> getMetadata() throws IOException {
      if (currentMetadata != null) return currentMetadata;
      loadMetadata();
      return currentMetadata;
    }

    public String getContent() throws IOException {
      if (currentContent != null) return currentContent;

      // Otherwise load it
      loadContent();
      return currentContent;
    }

    public Document getDocument() throws IOException {
      Document d = new Document();
      d.metadata = getMetadata();
      d.text = getContent();
      return d;
    }

    public ValueIterator getValueIterator() throws IOException {
      return new ValueIterator(this);
    }

    private void loadMetadata() throws IOException {
      currentStream = new VByteInput(iterator.getValueStream());
      int count = currentStream.readInt();
      currentMetadata = new HashMap<String, String>();
      for (int i = 0; i < count; i++) {
        String key = currentStream.readString();
        String value = currentStream.readString();
        currentMetadata.put(key, value);
      }
    }

    private void loadContent() throws IOException {
      if (currentMetadata == null) loadMetadata();
      currentContent = currentStream.readString();
    }

  }

  public class ValueIterator extends KeyToListIterator implements DataIterator<Document> {

    public ValueIterator(KeyIterator ki) {
      super(ki);
    }

    public String getEntry() throws IOException {
      return ((KeyIterator) iterator).getContent();
    }

    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document getData() {
      try {
        return ((KeyIterator) iterator).getDocument();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
}
