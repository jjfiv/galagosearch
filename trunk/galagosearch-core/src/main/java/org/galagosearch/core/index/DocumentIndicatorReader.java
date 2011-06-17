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
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * 
 * @author sjh
 */
public class DocumentIndicatorReader extends KeyValueReader {

  protected boolean def;
  protected Parameters manifest;

  public DocumentIndicatorReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
    this.manifest = this.reader.getManifest();
    def = Boolean.parseBoolean(this.manifest.get("default")); // this must exist
  }

  public DocumentIndicatorReader(GenericIndexReader r) {
    super(r);
  }

  public boolean getIndicator(int document) throws IOException {
    byte[] valueBytes = reader.getValueBytes(Utility.fromInt(document));
    if ((valueBytes == null) || (valueBytes.length == 0)) {
      return def;
    } else {
      return Utility.toBoolean(valueBytes);
    }
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("indicator", new NodeType(ValueIterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("indicator")) {
      return new ValueIterator(new KeyIterator(reader), node);
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyValueReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getKey(){
      return Integer.toString(getCurrentDocument());
    }
    
    public String getValueString() {
      try {
        return Boolean.toString(getCurrentIndicator());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public boolean skipToKey(int key) throws IOException {
      return skipToKey(Utility.fromInt(key));
    }

    public int getCurrentDocument() {
      return Utility.toInt(iterator.getKey());
    }

    public boolean getCurrentIndicator() throws IOException {
      byte[] valueBytes = iterator.getValueBytes();
      if ((valueBytes == null) || (valueBytes.length == 0)) {
        return def;
      } else {
        return Utility.toBoolean(valueBytes);
      }
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
    boolean defInst;
    
    public ValueIterator(KeyIterator it, Node node) {
      super(it);
      this.defInst = node.getParameters().get("default", def); // same as indri
    }

    public ValueIterator(KeyIterator it) {
      super(it);
      this.defInst = def; // same as indri
    }

    public String getEntry() throws IOException {
      return Integer.toString(((KeyIterator) iterator).getCurrentDocument());
    }

    public long totalEntries() {
      return manifest.get("keyCount",-1);
    }

    public int getIndicatorStatus() {
      return (getStatus() == true ? 1 : 0);
    }

    public boolean getStatus() {
      if (context.document == ((KeyIterator) iterator).getCurrentDocument()) {
        return defInst;
      } else {
        try {
          return ((KeyIterator) iterator).getCurrentIndicator();
        } catch (IOException ex) {
          Logger.getLogger(DocumentIndicatorReader.class.getName()).log(Level.SEVERE, null, ex);
          throw new RuntimeException("Failed to read indicator file.");
        }
      }
    }

    public boolean getStatus(int document) {
      if (document != ((KeyIterator) iterator).getCurrentDocument()) {
        return defInst;
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
