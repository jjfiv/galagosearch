// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.DataInput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.*;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.BufferedFileDataStream;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.VByteInput;

/**
 *
 * @author trevor
 */
public class ExtentIndexReader extends KeyListReader {

  public class KeyIterator extends KeyListReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getStringValue() {
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
  }

  public class ListIterator extends KeyListReader.ListIterator implements CountIterator, ExtentIterator {

    VByteInput data;
    BufferedFileDataStream dataStream;
    RandomAccessFile input;
    long startPosition, endPosition, dataLength;
    byte[] key;
    int documentCount;
    int options;
    int currentDocument;
    ExtentArray extents;
    int documentIndex;
    // skip support
    VByteInput skips;
    int skipDistance;
    int skipsRead;
    int numSkips;
    int nextSkipDocument;
    long lastSkipPosition;

    public ListIterator(GenericIndexReader.Iterator iterator) throws IOException {
      super(iterator);
    }

    public void reset(GenericIndexReader.Iterator iterator) throws IOException {
      startPosition = iterator.getValueStart();
      endPosition = iterator.getValueEnd();
      dataLength = iterator.getValueLength();
      key = iterator.getKey();
      RandomAccessFile input = iterator.getInput();
      reset();
    }

    public void reset() throws IOException {
      currentDocument = 0;
      extents.reset();
      documentCount = 0;
      documentIndex = 0;
      initialize();
    }

    public String getEntry() {
      StringBuilder builder = new StringBuilder();
      builder.append(getKey());
      builder.append(",");
      builder.append(currentDocument);
      for (int i = 0; i < extents.getPositionCount(); ++i) {
        builder.append(",(");
        builder.append(extents.getBuffer()[i].begin);
        builder.append(",");
        builder.append(extents.getBuffer()[i].end);
        builder.append(")");
      }
      return builder.toString();
    }

    private void initialize() throws IOException {
      input.seek(startPosition);
      DataInput stream = new VByteInput(input);

      options = stream.readInt();
      documentCount = stream.readInt();
      currentDocument = 0;
      documentIndex = 0;

      long dataStart = 0;
      long dataEnd = 0;

      // check for skips
      if ((options & ValueIterator.HAS_SKIPS) == ValueIterator.HAS_SKIPS) {
        skipDistance = stream.readInt();
        numSkips = stream.readInt();
        long remainingLength = stream.readLong();
        dataStart = input.getFilePointer();
        dataEnd = dataStart + remainingLength;
      } else {
        skipDistance = 0;
        numSkips = 0;
        dataStart = input.getFilePointer();
        dataEnd = endPosition;
      }

      // Load data stream
      dataStream = new BufferedFileDataStream(input, dataStart, dataEnd);
      data = new VByteInput(dataStream);

      // Now load skips if they're in
      if (skipDistance > 0) {
        skips = new VByteInput(new BufferedFileDataStream(input, dataEnd, endPosition));
        nextSkipDocument = skips.readInt();
        lastSkipPosition = 0;
        skipsRead = 0;
      } else {
        skips = null;
      }

      loadExtents();
    }

    public long totalEntries() {
      return documentCount;
    }

    public boolean nextEntry() throws IOException {
      extents.reset();
      documentIndex = Math.min(documentIndex + 1, documentCount);

      if (!isDone()) {
        loadExtents();
        return true;
      }
      return false;
    }

    public boolean hasMatch(int document) {
      return (!isDone() && intID() == document);
    }

    // If we have skips - it's go time
    @Override
    public boolean moveTo(int document) throws IOException {
      if (skips != null && document > nextSkipDocument) {
        // if we're here, we're skipping
        while (skipsRead < numSkips
                && document > nextSkipDocument) {
          skipOnce();
        }

        // Reposition the data stream
        dataStream.seek(lastSkipPosition);
        documentIndex = (int) (skipsRead * skipDistance) - 1;
      }

      // linear from here
      while (document > currentDocument && nextEntry());
      return hasMatch(document);
    }

    private void skipOnce() throws IOException {
      assert skipsRead < numSkips;

      // move forward once in the skip stream
      long currentSkipPosition = lastSkipPosition + skips.readInt();
      currentDocument = (int) nextSkipDocument;

      // May be at the end of the buffer
      if (skipsRead + 1 == numSkips) {
        nextSkipDocument = Integer.MAX_VALUE;
      } else {
        nextSkipDocument += skips.readInt();
      }
      skipsRead++;
      lastSkipPosition = currentSkipPosition;
    }

    private void loadExtents() throws IOException {
      currentDocument += data.readInt();
      int extentCount = data.readInt();
      int begin = 0;

      for (int i = 0; i < extentCount; i++) {
        int deltaBegin = data.readInt();
        int extentLength = data.readInt();
        long value = data.readLong();

        begin = deltaBegin + begin;
        int end = begin + extentLength;

        extents.add(currentDocument, begin, end, value);
      }
    }

    public int intID() {
      return currentDocument;
    }

    public int count() {
      return extents.getPositionCount();
    }

    public ExtentArray getData() {
      return extents;
    }

    public ExtentArray extents() {
      return extents;
    }

    public boolean isDone() {
      return (documentIndex >= documentCount);
    }

    public int compareTo(CountIterator other) {
      if (isDone() && !other.isDone()) {
        return 1;
      }
      if (other.isDone() && !isDone()) {
        return -1;
      }
      if (isDone() && other.isDone()) {
        return 0;
      }
      return intID() - other.intID();
    }
  }
  GenericIndexReader reader;

  public ExtentIndexReader(GenericIndexReader reader) throws IOException {
    super(reader);
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public ListIterator getListIterator() throws IOException {
    return new ListIterator(reader.getIterator());
  }

  public ListIterator getExtents(String term) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));

    if (iterator != null) {
      return new ListIterator(iterator);
    }
    return null;
  }

  public CountIterator getCounts(String term) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));

    if (iterator != null) {
      return new ListIterator(iterator);
    }
    return null;
  }

  public void close() throws IOException {
    reader.close();
  }

  public HashMap<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
    nodeTypes.put("extents", new NodeType(Iterator.class));
    return nodeTypes;
  }

  public StructuredIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("extents")) {
      return getExtents(node.getDefaultParameter());
    } else {
      throw new UnsupportedOperationException("Node type " + node.getOperator()
              + " isn't supported.");
    }
  }
}
