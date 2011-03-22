package org.galagosearch.core.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.DataIterator;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
public class AdjacencyNameReader extends NameReader {

  public AdjacencyNameReader(GenericIndexReader reader) throws IOException {
    super(reader);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("names", new NodeType(ValueIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names")) {
      return new ValueIterator(new KeyIterator(reader));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  @Override
  public String get(int identifier) throws IOException {
    return Utility.toString(reader.getValueBytes(Utility.fromInt(identifier)));
  }

  public class KeyIterator extends KeyValueReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      try {
        return iterator.getValueString();
      } catch (IOException ioe) {
        return "Unknown";
      }
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      return new ValueIterator(this);
    }
  }

  public class ValueIterator extends KeyToListIterator implements DataIterator<String> {

    public ValueIterator(KeyIterator ki) {
      super(ki);
    }

    public String getEntry() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return Utility.toInt(ki.getKeyBytes())+","+ ki.getValueString();
    }

    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getData() {
      try {
        return getEntry();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
}
