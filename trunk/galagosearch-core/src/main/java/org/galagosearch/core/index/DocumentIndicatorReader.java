// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.DocumentContext;
import org.galagosearch.core.retrieval.structured.IndicatorIterator;
import org.galagosearch.tupleflow.Utility;

/**
 * Reads documents lengths from a document lengths file.
 * Iterator provides a useful interface for dumping the contents of the file.
 *
 * offset is the first document number (for sequential sharding purposes)
 * 
 * @author trevor, sjh
 */
public class DocumentIndicatorReader extends KeyValueReader {
  boolean def;
    
  
  public DocumentIndicatorReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
    def = Boolean.parseBoolean(this.getManifest().get("default")); // this must exist
  }

  public DocumentIndicatorReader(GenericIndexReader r) {
    super(r);
  }

  public boolean getIndicator(int document) throws IOException {
    return Utility.toBoolean(reader.getValueBytes(Utility.fromInt(document)), def);
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("indicator", new NodeType(Iterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("indicator")) {
      return new ValueIterator(new KeyIterator(reader)) {};
    } else {
      throw new UnsupportedOperationException(
        "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyValueReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    public String getValueString() {
      try {
        StringBuilder sb = new StringBuilder();
        sb.append(Utility.toInt(iterator.getKey())).append(",");
        sb.append(Utility.uncompressInt(iterator.getValueBytes(), 0));
        return sb.toString();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public boolean moveToKey(int key) throws IOException {
      return moveToKey(Utility.fromInt(key));
    }

    public int getCurrentDocument() {
      return Utility.toInt(iterator.getKey());
    }

    public boolean getCurrentIndicator() throws IOException {
      return Utility.toBoolean(iterator.getValueBytes(), def);
    }

    public boolean isDone() {
      return iterator.isDone();
    }

    public ValueIterator getValueIterator() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  // needs to be an AbstractIndicator
  public class ValueIterator extends KeyToListIterator implements IndicatorIterator {
    DocumentContext context;
    
    public ValueIterator(KeyIterator it) {
      super(it);
    }

    public String getEntry() throws IOException {
      return Integer.toString(((KeyIterator) iterator).getCurrentDocument());
    }

    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public int getIndicatorStatus() {
      return (getStatus() == true ? 1 : 0);
    }

    public boolean getStatus() {
      if(context.document == ((KeyIterator) iterator).getCurrentDocument()){
        return def;
      } else {
        try {
          return ((KeyIterator) iterator).getCurrentIndicator();
        } catch (IOException ex) {
          Logger.getLogger(DocumentIndicatorReader.class.getName()).log(Level.SEVERE, null, ex);
          throw new RuntimeException("Failed to read indicator file.");
        }
      }
    }
    
    public boolean getStatus( int document ) {
      if(document != ((KeyIterator) iterator).getCurrentDocument()){
        return def;
      } else {
        try {
          return ((KeyIterator) iterator).getCurrentIndicator();
        } catch (IOException ex) {
          Logger.getLogger(DocumentIndicatorReader.class.getName()).log(Level.SEVERE, null, ex);
          throw new RuntimeException("Failed to read indicator file.");
        }
      }
    }
    
    public DocumentContext getContext() {
      return context;
    }

    public void setContext(DocumentContext context) {
      this.context = context;
    }
  }
}
