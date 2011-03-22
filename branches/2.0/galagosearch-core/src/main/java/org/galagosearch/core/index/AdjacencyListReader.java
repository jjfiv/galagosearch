/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index;

import gnu.trove.TObjectDoubleHashMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.DataIterator;
import org.galagosearch.core.retrieval.structured.DocumentContext;
import org.galagosearch.core.retrieval.structured.ScoreValueIterator;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.VByteInput;

/**
 *
 * @author irmarc
 */
public class AdjacencyListReader extends KeyListReader {

  public class KeyIterator extends KeyListReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      IntegerListIterator it;
      long count = -1;
      try {
        it = new IntegerListIterator(iterator);
        count = it.totalEntries();
      } catch (IOException ioe) {
      }

      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(iterator.getKey())).append(", List Value: size=");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    public ValueIterator getValueIterator() throws IOException {
      if (idType.equals("int")) {
        return new IntegerListIterator(iterator);
      } else {
        return new DataListIterator(iterator);
      }
    }
  }

  public class IntegerListIterator extends KeyListReader.ListIterator
          implements ScoreValueIterator {

    VByteInput stream;
    int neighborhood;
    int index;
    int currentIdentifier;
    double currentScore;
    DocumentContext context;

    public IntegerListIterator(GenericIndexReader.Iterator iterator) throws IOException {
      reset(iterator);
    }

    void read() throws IOException {
      index += 1;

      if (index < neighborhood) {
        currentIdentifier += stream.readInt();
        currentScore = stream.readDouble();
      }
    }

    public String getEntry() {
      StringBuilder builder = new StringBuilder();

      builder.append(getKey());
      builder.append(",");
      builder.append(currentIdentifier);
      builder.append(",");
      builder.append(currentScore);

      return builder.toString();
    }

    public boolean next() throws IOException {
      read();
      if (!isDone()) {
        return true;
      }
      return false;
    }

    public void reset(GenericIndexReader.Iterator iterator) throws IOException {
      DataStream buffered = iterator.getValueStream();
      stream = new VByteInput(buffered);
      neighborhood = stream.readInt();
      index = -1;
      currentIdentifier = 0;
      if (neighborhood > 0) {
        read();
      }
    }

    public void reset() throws IOException {
      throw new UnsupportedOperationException("This iterator does not reset without the parent KeyIterator.");
    }

    public void setContext(DocumentContext dc) {
      this.context = dc;
    }

    public DocumentContext getContext() {
      return context;
    }

    public int currentCandidate() {
      return currentIdentifier;
    }

    public boolean hasMatch(int id) {
      return id == currentIdentifier;
    }

    public boolean moveTo(int document) throws IOException {
      while (!isDone() && document > currentIdentifier) {
        read();
      }
      return hasMatch(document);
    }

    public void movePast(int document) throws IOException {
      while (!isDone() && document >= currentIdentifier) {
        read();
      }
    }

    public double score(DocumentContext dc) {
      if (currentIdentifier == dc.document) {
        return currentScore;
      }
      return Double.NEGATIVE_INFINITY;
    }

    public double score() {
      if (currentIdentifier == context.document) {
        return currentScore;
      }
      return Double.NEGATIVE_INFINITY;
    }

    public boolean isDone() {
      return index >= neighborhood;
    }

    public long totalEntries() {
      return neighborhood;
    }

    public double maximumScore() {
      return Double.POSITIVE_INFINITY;
    }

    public double minimumScore() {
      return Double.NEGATIVE_INFINITY;
    }

    public TObjectDoubleHashMap<String> parameterSweepScore() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
  
  public class Neighbor {
    public Neighbor(byte[] id, double w) { identifier = id; weight = w; }
    public byte[] identifier;
    double weight;
    public String toString() { return "<"+Utility.toString(identifier) + "," + weight + ">"; }
  }

  public class DataListIterator extends KeyListReader.ListIterator
          implements DataIterator<Neighbor>, ValueIterator {

    VByteInput stream;
    int neighborhood;
    int index;
    int currentIdentifier;
    Neighbor currentNeighbor;
    DocumentContext context;

    public DataListIterator(GenericIndexReader.Iterator iterator) throws IOException {
      reset(iterator);
    }

    void read() throws IOException {
      index += 1;

      if (index < neighborhood) {
        int size = stream.readInt();
        byte[] buffer = new byte[size];
        stream.readFully(buffer);
        double score = stream.readDouble();
        currentNeighbor = new Neighbor(buffer, score);
        currentIdentifier++;
      }
    }

    public String getEntry() {
      StringBuilder builder = new StringBuilder();

      builder.append(getKey());
      builder.append(",");
      builder.append(currentNeighbor.toString());
      return builder.toString();
    }

    public boolean next() throws IOException {
      read();
      if (!isDone()) {
        return true;
      }
      return false;
    }

    public void reset(GenericIndexReader.Iterator iterator) throws IOException {
      DataStream buffered = iterator.getValueStream();
      stream = new VByteInput(buffered);
      neighborhood = stream.readInt();
      index = -1;
      key = iterator.getKey();
      currentIdentifier = 0;
      if (neighborhood > 0) {
        read();
      }
    }

    public void reset() throws IOException {
      throw new UnsupportedOperationException("This iterator does not reset without the parent KeyIterator.");
    }

    public int currentCandidate() {
      return currentIdentifier;
    }

    public boolean hasMatch(int id) {
      return id == currentIdentifier;
    }

    public boolean moveTo(int index) throws IOException {
      while (!isDone() && index > currentIdentifier) {
        read();
      }
      return hasMatch(index);
    }

    public void movePast(int index) throws IOException {
      while (!isDone() && index >= currentIdentifier) {
        read();
      }
    }

    public boolean isDone() {
      return index >= neighborhood;
    }

    public long totalEntries() {
      return neighborhood;
    }

    public Neighbor getData() {
      return currentNeighbor;
    }
  }

  String idType;

  public AdjacencyListReader(String pathname) throws FileNotFoundException, IOException {
    super(pathname);
    idType = reader.getManifest().get("idType");
  }

  public AdjacencyListReader(GenericIndexReader reader) {
    super(reader);
    idType = reader.getManifest().get("idType");
  }


  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public ValueIterator getListIterator() throws IOException {
    if (idType.equals("int")) {
      return new IntegerListIterator(reader.getIterator());
    } else {
      return new DataListIterator(reader.getIterator());
    }
  }

  public ValueIterator getScores(String term) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));
    if (idType.equals("int")) {
      return new IntegerListIterator(reader.getIterator());
    } else {
      return new DataListIterator(reader.getIterator());
    }
  }

  public void close() throws IOException {
    reader.close();
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
    nodeTypes.put("neighbors", new NodeType(Iterator.class));
    return nodeTypes;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("neighbors")) {
      return getScores(node.getDefaultParameter());
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }
}
