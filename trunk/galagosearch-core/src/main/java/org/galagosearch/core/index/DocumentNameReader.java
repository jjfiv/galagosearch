// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.DataIterator;

import org.galagosearch.tupleflow.Utility;

/**
 * Reads a binary file of document names produced by DocumentNameWriter2
 * 
 * @author sjh
 */
public class DocumentNameReader extends NameReader {

  public boolean isForward;

  /** Creates a new instance of DocumentNameReader */
  public DocumentNameReader(String fileName) throws IOException {
    this(GenericIndexReader.getIndexReader(fileName));
  }

  public DocumentNameReader(GenericIndexReader r) {
    super(r);
    isForward = (reader.getManifest().get("order").equals("forward"));
  }

  // gets the document name of the internal id index.
  public String getDocumentName(int index) throws IOException {
    if (isForward) {
      byte[] data = reader.getValueBytes(Utility.fromInt(index));
      if (data == null) {
        throw new IOException("Unknown Document Number : " + index);
      }
      return Utility.toString(data);
    } else {
      throw new UnsupportedOperationException("This index file does not support doc int -> doc name mappings");
    }
  }

  // gets the document id for some document name
  public int getDocumentId(String documentName) throws IOException {
    if (!isForward) {
      byte[] data = reader.getValueBytes(Utility.fromString(documentName));
      if (data == null) {
        throw new IOException("Unknown Document Name : " + documentName);
      }
      return Utility.toInt(data);
    } else {
      throw new UnsupportedOperationException("This index file does not support doc name -> doc int mappings");
    }
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader, isForward);
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("names", new NodeType(ValueIterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names") && isForward) {
      return new ValueIterator(new KeyIterator(reader, isForward));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyValueReader.Iterator {

    boolean forwardLookup;
    GenericIndexReader input;
    GenericIndexReader.Iterator iterator;

    public KeyIterator(GenericIndexReader input, boolean forwardLookup) throws IOException {
      super(input);
      this.forwardLookup = forwardLookup;
    }

    public boolean isForward() {
      return forwardLookup;
    }

    public boolean skipToKey(int identifier) throws IOException {
      if (forwardLookup) {
        return skipToKey(Utility.fromInt(identifier));
      } else {
        throw new UnsupportedOperationException("Direction is wrong.");
      }
    }

    public boolean findKey(String name) throws IOException {
      if (forwardLookup) {
        throw new UnsupportedOperationException("Direction is wrong.");
      } else {
        return findKey(Utility.fromString(name));
      }
    }

    public String getCurrentName() throws IOException {
      if (forwardLookup) {
        return Utility.toString(getValueBytes());
      } else {
        return Utility.toString(getKeyBytes());
      }
    }

    public int getCurrentIdentifier() throws IOException {
      if (!forwardLookup) {
        return Utility.toInt(getValueBytes());
      } else {
        return Utility.toInt(getKeyBytes());
      }
    }

    public String getValueString() {
      try {
        if (forwardLookup) {
          return Utility.toString(getValueBytes());
        } else {
          return Integer.toString(Utility.toInt(getValueBytes()));
        }
      } catch (IOException e) {
        return "Unknown";
      }
    }

    public String getKey() {
      if (forwardLookup) {
        return Integer.toString(Utility.toInt(getKeyBytes()));
      } else {
        return Utility.toString(getKeyBytes());
      }
    }

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
      StringBuilder sb = new StringBuilder();
      if (ki.isForward()) {
        sb.append(ki.getCurrentIdentifier());
        sb.append(",");
        sb.append(ki.getCurrentName());
      } else {
        sb.append(ki.getCurrentName());
        sb.append(",");
        sb.append(ki.getCurrentIdentifier());
      }
      return sb.toString();
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
