/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index;

import java.io.DataInput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.StructuredIterator;
import org.galagosearch.tupleflow.BufferedFileDataStream;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.VByteInput;

/**
 *
 * @author marc
 */
public class TopDocsReader extends KeyListReader {
  private Logger LOG = Logger.getLogger(getClass().toString());
  IndexReader reader;

  public class TopDocument implements Cloneable {

    public int document;
    public int count;
    public int length;

    public boolean equals(Object o) {
      if (o instanceof TopDocument == false) {
        return false;
      }
      return (this.document == ((TopDocument) o).document);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(document);
      sb.append(",");
      sb.append(count);
      sb.append(",");
      sb.append(length);
      return sb.toString();
    }

    public TopDocument clone() {
      TopDocument copy = new TopDocument();
      copy.document = this.document;
      copy.count = this.count;
      copy.length = this.length;
      return copy;
    }
  }

  public class KeyIterator extends KeyListReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      ListIterator it;
      long count = -1;
      try {
        it = new ListIterator(iterator);
        count = it.totalEntries();
      } catch (IOException ioe) {}

      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(iterator.getKey())).append(", List Value: size=");
      if (count > 0) sb.append(count);
      else sb.append("Unknown");
      return sb.toString();
    }

    public ValueIterator getValueIterator() throws IOException {
      return new ListIterator(iterator);
    }
  }

  public class ListIterator extends KeyListReader.ListIterator {

    IndexReader.Iterator iterator;
    int options;
    TopDocument currentTopDoc;
    int lastDocument;
    int index;
    int numEntries;
    VByteInput data;

    // To support resetting
    long startPosition, endPosition, dataLength;
    RandomAccessFile input;
    byte[] key;

    public ListIterator(GenericIndexReader.Iterator it) throws IOException {
      reset(it);
    }

    private void initialize() throws IOException {
      input.seek(startPosition);
      DataInput stream = new VByteInput(input);

      // header info
      numEntries = stream.readInt();
      index = 0;
      lastDocument = 0;
      long dataStart = input.getFilePointer();
      long dataEnd = endPosition;
      data = new VByteInput(new BufferedFileDataStream(input, dataStart, dataEnd));
      next();
    }

    public long totalEntries() {
      return numEntries;
    }

    public boolean isDone() {
      return (index >= numEntries);
    }

    public int currentCandidate() {
      if (currentTopDoc != null) {
        return currentTopDoc.document;
      } else {
        return Integer.MAX_VALUE;
      }
    }

    public TopDocument getCurrentTopDoc() {
      return currentTopDoc;
    }

    public boolean next() throws IOException {
      if (!isDone()) {
        currentTopDoc = new TopDocument();
        currentTopDoc.document = lastDocument + data.readInt();
        lastDocument = currentTopDoc.document;
        currentTopDoc.count = data.readInt();
        currentTopDoc.length = data.readInt();
        index++;
        return true;
      } else {
        currentTopDoc = null;
        return false;
      }
    }

    public void reset(GenericIndexReader.Iterator iterator) throws IOException {
      startPosition = iterator.getValueStart();
      endPosition = iterator.getValueEnd();
      input = iterator.getInput();
      key = iterator.getKey();
      reset();
    }

    public void reset() throws IOException {
      initialize();
    }

    public String getEntry() {
      StringBuilder sb = new StringBuilder();
      sb.append(getKey());
      sb.append(",");
      if (currentTopDoc != null) {
        sb.append(currentTopDoc.toString());
      } else {
        sb.append("NULL");
      }
      return sb.toString();
    }

    public boolean hasMatch(int document) {
      return (!isDone() && currentTopDoc.document == document);
    }

    public boolean moveTo(int document) throws IOException {
      int curDoc = currentTopDoc.document;
      int curCount = currentTopDoc.count;
      int curLength = currentTopDoc.length;
      while (!isDone() && curDoc < document) {
        curDoc += data.readInt();
        curCount = data.readInt();
        curLength = data.readInt();
        index++;
      }
      if (isDone()) {
        currentTopDoc = null;
      } else {
        currentTopDoc = new TopDocument();
        currentTopDoc.document = curDoc;
        currentTopDoc.count = curCount;
        currentTopDoc.length = curLength;
      }
      return hasMatch(document);
    }
  }

  public TopDocsReader(GenericIndexReader r) {
    super(r);
  }

  public ListIterator getTopDocs(String term) throws IOException {
    IndexReader.Iterator it = reader.getIterator(Utility.fromString(term));

    if (it != null) {
      return new ListIterator(it);
    } else {
      return null;
    }
  }

  public void close() throws IOException {
    reader.close();
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
    nodeTypes.put("topdocs", new NodeType(Iterator.class));
    return nodeTypes;
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public ListIterator getListIterator() throws IOException {
    return new ListIterator(reader.getIterator());
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("topdocs")) {
      return getTopDocs(node.getParameters().get("term"));
    } else {
      throw new UnsupportedOperationException("Node type " + node.getOperator()
              + " isn't supported.");
    }
  }
}
