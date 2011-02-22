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

  public Iterator getIterator() throws IOException {
    return new Iterator(reader, isForward);
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("names", new NodeType(Iterator.class));
    return types;
  }

  public KeyIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names")) {
      return new Iterator(reader, isForward);
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class Iterator extends KeyValueReader.Iterator {

    boolean forwardLookup;
    GenericIndexReader input;
    GenericIndexReader.Iterator iterator;
    KeyValuePair current;

    public Iterator(GenericIndexReader input, boolean forwardLookup) throws IOException {
      super(input);
      this.forwardLookup = forwardLookup;
    }

    public String getStringValue() {
      if (forwardLookup) {
        return Utility.toInt(current.key) + ", " + Utility.toString(current.value);
      } else {
        return Utility.toInt(current.value) + ", " + Utility.toString(current.key);
      }
    }

    public NumberedDocumentData getDocumentData() throws IOException {
      if (forwardLookup) {
        return new NumberedDocumentData(Utility.toString(current.value), "", Utility.toInt(current.key), 0);
      } else {
        return new NumberedDocumentData(Utility.toString(current.key), "", Utility.toInt(current.value), 0);
      }
    }

    public String getKey() {
      if (forwardLookup) {
        return Integer.toString(Utility.toInt(current.key));
      } else {
        return Utility.toString(current.key);
      }
    }

    public boolean isDone() {
      return iterator.isDone();
    }
  }
}
