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
import org.galagosearch.core.retrieval.structured.ScoreIterator;
import org.galagosearch.core.retrieval.structured.ScoreValueIterator;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * 
 * @author sjh
 */
public class DocumentPriorReader extends KeyValueReader {

  private double def;
  protected Parameters manifest;

  public DocumentPriorReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
    def = Double.parseDouble(this.getManifest().get("default")); // this must exist
  }

  public DocumentPriorReader(GenericIndexReader r) {
    super(r);
    this.manifest = this.reader.getManifest();
  }

  public double getPrior(int document) throws IOException {
    byte[] valueBytes = reader.getValueBytes(Utility.fromInt(document));
    if ((valueBytes == null) || (valueBytes.length == 0)) {
      return def;
    } else {
      return Utility.toDouble(valueBytes);
    }
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("prior", new NodeType(ValueIterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    double definst = node.getParameters().get("default", def);
    if (node.getOperator().equals("prior")) {
      return new ValueIterator(new KeyIterator(reader), definst);
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

    public double getCurrentScore() throws IOException {
      byte[] valueBytes = iterator.getValueBytes();
      if ((valueBytes == null) || (valueBytes.length == 0)) {
        return def;
      } else {
        return Utility.toDouble(valueBytes);
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
  public class ValueIterator extends KeyToListIterator implements ScoreValueIterator {

    DocumentContext context;
    double defInst;
    
    public ValueIterator(KeyIterator it, double defInst) {
      super(it);
      this.defInst = defInst;
    }

    public String getEntry() throws IOException {
      return Integer.toString(((KeyIterator) iterator).getCurrentDocument());
    }

    public DocumentContext getContext() {
      return context;
    }

    public void setContext(DocumentContext context) {
      this.context = context;
    }

    public long totalEntries() {
      return manifest.get("keyCount", -1);
    }

    public double score() {
      return this.score(this.context);
    }

    public double score(DocumentContext context) {
      try {
        if (this.currentCandidate() == context.document) {
          byte[] valueBytes = iterator.getValueBytes();
          if ((valueBytes == null) || (valueBytes.length == 0)) {
            return defInst;
          } else {
            return Utility.toDouble(valueBytes);
          }
        } else {
          return defInst;
        }
      } catch (IOException ex) {
        Logger.getLogger(DocumentPriorReader.class.getName()).log(Level.SEVERE, null, ex);
        throw new RuntimeException( ex );
      }
    }

    public double maximumScore() {
      return manifest.get("maxScore", Double.POSITIVE_INFINITY);
    }

    public double minimumScore() {
      return manifest.get("minScore", Double.NEGATIVE_INFINITY);
    }
  }
}
