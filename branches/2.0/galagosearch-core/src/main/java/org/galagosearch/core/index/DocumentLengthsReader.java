// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.index.PositionIndexReader.TermCountIterator;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Utility;

/**
 * Reads documents lengths from a document lengths file.
 * Iterator provides a useful interface for dumping the contents of the file.
 *
 * offset is the first document number (for sequential sharding purposes)
 * 
 * @author trevor, sjh
 */
public class DocumentLengthsReader extends KeyValueReader {

  public DocumentLengthsReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
  }

  public DocumentLengthsReader(GenericIndexReader r) {
    super(r);
  }

  public int getLength(int document) throws IOException {
    return Utility.uncompressInt(reader.getValueBytes(Utility.fromInt(document)), 0);
  }

  public Iterator getIterator() throws IOException {
    return new Iterator(reader);
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("lengths", new NodeType(Iterator.class));
    return types;
  }

  public KeyIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("lengths")) {
      return new Iterator(reader);
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class Iterator extends KeyValueReader.Iterator {

    public Iterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    public String getStringValue() {
      try {
        StringBuilder sb = new StringBuilder();
        sb.append(Utility.toInt(iterator.getKey())).append(",");
        sb.append(Utility.uncompressInt(iterator.getValueBytes(), 0));
        return sb.toString();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public void skipToKey(int key) throws IOException {
      byte[] bkey = Utility.fromInt(key);
      iterator.skipTo(bkey);
    }

    public int getIntValue() throws IOException {
      return (Utility.uncompressInt(iterator.getValueBytes(), 0));
    }

    public long getLongValue() throws IOException {
      return ((long) getIntValue());
    }

    public int getCurrentDocument() {
      return Utility.toInt(iterator.getKey());
    }

    public boolean isDone() {
      return iterator.isDone();
    }
  }
}
