// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.index.PositionIndexReader.TermCountIterator;
import org.galagosearch.core.index.PositionIndexReader.TermExtentIterator;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.types.KeyValuePair;

import org.galagosearch.core.types.NumberedDocumentData;
import org.galagosearch.tupleflow.Utility;

/**
 * Reads a binary file of document names produced by DocumentNameWriter2.
 * BulkTrees are used 
 * 
 * 
 * Reverse lookup is provided 
 * 
 * @author sjh
 */
public class DocumentNameReader extends KeyValueReader {

  boolean isForward;

  /** Creates a new instance of DocumentNameReader */
  public DocumentNameReader(String fileName) throws IOException {
    super(fileName);
    isForward = (reader.getManifest().get("order").equals("forward"));
  }

  public DocumentNameReader(GenericIndexReader r) {
    super(r);
    isForward = (reader.getManifest().get("order").equals("forward"));
  }

  // gets the document name of the internal id index.
  public String get(int index) throws IOException {
    if (isForward) {
      byte[] data = reader.getValueBytes(Utility.fromInt(index));
      if (data == null) {
        throw new IOException("Unknown Document Number : " + index);
      }
      return Utility.toString(data);
    } else {
      throw new UnsupportedOperationException("This direction does not support int -> name mappings");
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
      throw new UnsupportedOperationException("This direction does not support name -> int mappings");
    }
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader, isForward);
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("names", new NodeType(Iterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names")) {
      return new ValueIterator(new KeyIterator(reader, isForward));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends NumberedDocumentDataIterator {

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

    public boolean moveToKey(int identifier) throws IOException {
      if (forwardLookup) {
        return moveToKey(Utility.fromInt(identifier));
      } else {
        throw new UnsupportedOperationException("Direction is wrong.");
      }
    }

    public boolean moveToKey(String name) throws IOException {
      if (forwardLookup) {
        throw new UnsupportedOperationException("Direction is wrong.");
      } else {
        return moveToKey(Utility.fromString(name));
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
        byte[] key = getKeyBytes();
        byte[] value = getValueBytes();
        if (forwardLookup) {
          return Utility.toInt(key) + ", " + Utility.toString(value);
        } else {
          return Utility.toInt(value) + ", " + Utility.toString(key);
        }
      } catch (IOException ioe) {
        return "Unknown";
      }
    }

    public NumberedDocumentData getDocumentData() throws IOException {
      byte[] key = getKeyBytes();
      byte[] value = getValueBytes();

      if (forwardLookup) {
        return new NumberedDocumentData(Utility.toString(value), "", Utility.toInt(key), 0);
      } else {
        return new NumberedDocumentData(Utility.toString(key), "", Utility.toInt(value), 0);
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
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public class ValueIterator extends KeyToListIterator {

    public ValueIterator(KeyIterator ki) {
      super(ki);
    }

    public String getEntry() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return ki.getValueString();
    }

    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
