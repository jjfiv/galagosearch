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
    if (node.getOperator().equals("prior")) {
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
        return Double.toString(getCurrentScore());
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

    public double getCurrentScore() throws IOException {
      byte[] valueBytes = iterator.getValueBytes();
      if ((valueBytes == null) || (valueBytes.length == 0)) {
        return def;
      } else {
        return Utility.toDouble(valueBytes);
      }
    }

    public ValueIterator getValueIterator() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  // needs to be an AbstractIndicator
  public class ValueIterator extends KeyToListIterator implements ScoreValueIterator {

    DocumentContext context;
    double defInst;
    double minScore;
    
    public ValueIterator(KeyIterator it, Node node) {
      super(it);
      this.defInst = node.getParameters().get("default", def);
      this.minScore = node.getParameters().get("minScore", Math.log(0.0000000001)); // same as indri
    }

    public ValueIterator(KeyIterator it) {
      super(it);
      this.defInst = def;
      this.minScore = Math.log(0.0000000001); // same as indri
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

    @Override
    public boolean hasMatch(int identifier){
      return (! this.isDone() 
              && identifier == this.currentCandidate() 
              && this.score() > this.minScore);
    }
    
    public double maximumScore() {
      return manifest.get("maxScore", Double.POSITIVE_INFINITY);
    }

    public double minimumScore() {
      return manifest.get("minScore", Double.NEGATIVE_INFINITY);
    }
  }
}
