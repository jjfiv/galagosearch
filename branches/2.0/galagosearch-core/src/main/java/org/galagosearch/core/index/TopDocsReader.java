// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.DataInput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.DocumentOrderedIterator;
import org.galagosearch.core.retrieval.structured.IndexIterator;
import org.galagosearch.core.util.CallTable;
import org.galagosearch.tupleflow.BufferedFileDataStream;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.VByteInput;

/**
 *
 * @author marc
 */
public class TopDocsReader implements StructuredIndexPartReader {
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

  public class Iterator implements DocumentOrderedIterator, IndexIterator, Comparable<Iterator> {

    IndexReader.Iterator iterator;
    int options;
    TopDocument currentTopDoc;
    int lastDocument;
    int index;
    int numEntries;
    VByteInput data;

    public Iterator(IndexReader.Iterator it) throws IOException {
      iterator = it;
      options = 0x0; // no options here
      initialize();
    }

    private void initialize() throws IOException {
      long startPosition = iterator.getValueStart();
      long endPosition = iterator.getValueEnd();

      RandomAccessFile input = iterator.getInput();
      input.seek(startPosition);
      DataInput stream = new VByteInput(input);

      // header info
      numEntries = stream.readInt();
      index = 0;
      lastDocument = 0;
      long dataStart = input.getFilePointer();
      long dataEnd = endPosition;
      data = new VByteInput(new BufferedFileDataStream(input, dataStart, dataEnd));
      nextEntry();
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

    public void nextEntry() throws IOException {
      if (!isDone()) {
        currentTopDoc = new TopDocument();
        currentTopDoc.document = lastDocument + data.readInt();
        lastDocument = currentTopDoc.document;
        currentTopDoc.count = data.readInt();
        currentTopDoc.length = data.readInt();
        index++;
      } else {
        currentTopDoc = null;
      }
    }

    public void reset() throws IOException {
      initialize();
    }

    public boolean skipTo(byte[] key) throws IOException {
      iterator.skipTo(key);
      if (Utility.compare(key, iterator.getKey()) == 0) {
        reset();
        return true;
      }
      return false;
    }

    public String getRecordString() {
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

    public boolean nextRecord() throws IOException {
      nextEntry();
      if (!isDone()) {
        return true;
      }
      if (iterator.nextKey()) {
        reset();
        return true;
      } else {
        return false;
      }
    }

    public String getKey() {
      return Utility.toString(iterator.getKey());
    }

    public byte[] getKeyBytes() throws IOException {
      return iterator.getKey();
    }

    public boolean hasMatch(int document) {
      return (!isDone() && currentTopDoc.document == document);
    }

    public void moveTo(int document) throws IOException {
      if (isDone() || currentTopDoc.document >= document) {
        return;
      }
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
    }

    public void movePast(int document) throws IOException {
      moveTo(document + 1);
    }

    public boolean skipToDocument(int document) throws IOException {
      moveTo(document);
      return (hasMatch(document));
    }

    public int compareTo(Iterator that) {
      return (this.currentCandidate() - that.currentCandidate());
    }
  }

  public TopDocsReader(IndexReader r) {
    reader = r;
  }

  public Iterator getTopDocs(String term) throws IOException {
    IndexReader.Iterator it = reader.getIterator(Utility.fromString(term));

    if (it != null) {
      return new Iterator(it);
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

  public Iterator getIterator() throws IOException {
    return new Iterator(reader.getIterator());
  }

  public IndexIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("topdocs")) {
      return getTopDocs(node.getParameters().get("term"));
    } else {
      throw new UnsupportedOperationException("Node type " + node.getOperator()
              + " isn't supported.");
    }
  }
}
