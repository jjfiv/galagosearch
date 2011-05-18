// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.galagosearch.core.index.TopDocsReader.TopDocument;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.CountValueIterator;
import org.galagosearch.core.retrieval.structured.ContextualIterator;
import org.galagosearch.core.retrieval.structured.DocumentContext;
import org.galagosearch.core.retrieval.structured.ExtentValueIterator;
import org.galagosearch.core.retrieval.structured.TopDocsContext;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.BufferedFileDataStream;
import org.galagosearch.tupleflow.DataStream;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.VByteInput;

/**
 * Reads a simple positions-based index, where each inverted list in the
 * index contains both term count information and term position information.
 * The term counts data is stored separately from term position information for
 * faster query processing when no positions are needed.
 * 
 * (12/16/2010, irmarc): In order to facilitate faster count-only processing,
 *                        the default iterator created will not even open the
 *                        positions list when iterating. This is an interesting
 *                        enough change that there are now two versions of the iterator
 *
 * @author trevor, irmarc
 */
public class PositionIndexReader extends KeyListReader implements AggregateReader {

  public class KeyIterator extends KeyListReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      TermCountIterator it;
      long count = -1;
      try {
        it = new TermCountIterator(iterator);
        count = it.count();
      } catch (IOException ioe) {
      }
      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(getKeyBytes())).append(",");
      sb.append("list of size: ");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    public ValueIterator getValueIterator() throws IOException {
      return new TermExtentIterator(iterator);
    }
  }

  public interface AggregateIterator {
        public long totalEntries();
        public long totalPositions();
  }

  public class TermExtentIterator extends KeyListReader.ListIterator
      implements AggregateIterator, CountValueIterator, ExtentValueIterator, ContextualIterator {

    DocumentContext context;
    int documentCount;
    int totalPositionCount;
    VByteInput documents;
    VByteInput counts;
    VByteInput positions;
    int documentIndex;
    int currentDocument;
    int currentCount;
    ExtentArray extentArray;
    long startPosition, endPosition;
    RandomAccessFile input;
    // to support skipping
    VByteInput skips;
    VByteInput skipPositions;
    DataStream skipPositionsStream;
    DataStream documentsStream;
    DataStream countsStream;
    DataStream positionsStream;
    int skipDistance;
    int skipResetDistance;
    long numSkips;
    long skipsRead;
    long nextSkipDocument;
    long lastSkipPosition;
    long documentsByteFloor;
    long countsByteFloor;
    long positionsByteFloor;

    TermExtentIterator(GenericIndexReader.Iterator iterator) throws IOException {
      extentArray = new ExtentArray();
      reset(iterator);
    }

    // Initialization method.
    //
    // Even though we check for skips multiple times, in terms of how the data is loaded
    // its easier to do the parts when appropriate
    protected void initialize() throws IOException {
      input.seek(startPosition);
      DataInput stream = new VByteInput(input);

      // metadata
      int options = stream.readInt();
      documentCount = stream.readInt();
      totalPositionCount = stream.readInt();
      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipDistance = stream.readInt();
        skipResetDistance = stream.readInt();
        numSkips = stream.readLong();
      }

      // segment lengths
      long documentByteLength = stream.readLong();
      long countsByteLength = stream.readLong();
      long positionsByteLength = stream.readLong();
      long skipsByteLength = 0;
      long skipPositionsByteLength = 0;

      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipsByteLength = stream.readLong();
        skipPositionsByteLength = stream.readLong();
      }

      long documentStart = input.getFilePointer();
      long documentEnd = documentStart + documentByteLength;

      long countsStart = documentEnd;
      long countsEnd = countsStart + countsByteLength;

      long positionsStart = countsEnd;
      long positionsEnd = positionsStart + positionsByteLength;


      if ((options & HAS_SKIPS) == HAS_SKIPS) {

        long skipsStart = positionsEnd;
        long skipsEnd = skipsStart + skipsByteLength;

        long skipPositionsStart = skipsEnd;
        long skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;

        assert skipPositionsEnd == endPosition;

        // we do these here b/c of scoping issues w/ the variables above
        documentsStream = new BufferedFileDataStream(input, documentStart, documentEnd);
        documents = new VByteInput(documentsStream);
        countsStream = new BufferedFileDataStream(input, countsStart, countsEnd);
        counts = new VByteInput(countsStream);
        positionsStream = new BufferedFileDataStream(input, positionsStart, positionsEnd);
        positions = new VByteInput(positionsStream);
        skips = new VByteInput(new BufferedFileDataStream(input, skipsStart, skipsEnd));
        skipPositionsStream = new BufferedFileDataStream(input, skipPositionsStart,
                skipPositionsEnd);
        skipPositions = new VByteInput(skipPositionsStream);

        // load up
        nextSkipDocument = skips.readInt();
        documentsByteFloor = 0;
        countsByteFloor = 0;
        positionsByteFloor = 0;
      } else {
        assert positionsEnd == endPosition;
        skips = null;
        skipPositions = null;
        documents = new VByteInput(new BufferedFileDataStream(input, documentStart, documentEnd));
        counts = new VByteInput(new BufferedFileDataStream(input, countsStart, countsEnd));
        positions = new VByteInput(new BufferedFileDataStream(input, positionsStart, positionsEnd));

      }

      documentIndex = 0;
      loadExtents();
    }

    // Loads up a single set of positions for an intID. Basically it's the
    // load that needs to be done when moving forward one in the posting list.
    private void loadExtents() throws IOException {
      currentDocument += documents.readInt();
      currentCount = counts.readInt();
      extentArray.reset();

      int position = 0;
      for (int i = 0; i < currentCount; i++) {
        position += positions.readInt();
        extentArray.add(currentDocument, position, position + 1);
      }
    }

    public String getEntry() {
      StringBuilder builder = new StringBuilder();

      builder.append(getKey());
      builder.append(",");
      builder.append(currentDocument);
      for (int i = 0; i < extentArray.getPositionCount(); ++i) {
        builder.append(",");
        builder.append(extentArray.getBuffer()[i].begin);
      }

      return builder.toString();
    }

    public void reset(GenericIndexReader.Iterator iterator) throws IOException {
      key = iterator.getKey();
      dataLength = iterator.getValueLength();
      startPosition = iterator.getValueStart();
      endPosition = iterator.getValueEnd();

      input = iterator.getInput();
      reset();
    }

    public void reset() throws IOException {
      currentDocument = 0;
      currentCount = 0;
      extentArray.reset();
      initialize();
    }

    public boolean next() throws IOException {      
      documentIndex = Math.min(documentIndex + 1, documentCount);
      if (!isDone()) {
        loadExtents();
        return true;
      }
      return false;
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
        repositionMainStreams();
      }

      // Linear from here
      while (document > currentDocument && next());
      return hasMatch(document);
    }

    // This only moves forward in tier 1, reads from tier 2 only when
    // needed to update floors
    //
    private void skipOnce() throws IOException {
      assert skipsRead < numSkips;
      long currentSkipPosition = lastSkipPosition + skips.readInt();

      if (skipsRead % skipResetDistance == 0) {
        // Position the skip positions stream
        skipPositionsStream.seek(currentSkipPosition);

        // now set the floor values
        documentsByteFloor = skipPositions.readInt();
        countsByteFloor = skipPositions.readInt();
        positionsByteFloor = skipPositions.readLong();
      }
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

    private void repositionMainStreams() throws IOException {
      // If we just reset the floors, don't read the 2nd tier again
      if ((skipsRead - 1) % skipResetDistance == 0) {
        documentsStream.seek(documentsByteFloor);
        countsStream.seek(countsByteFloor);
        positionsStream.seek(positionsByteFloor);
      } else {
        skipPositionsStream.seek(lastSkipPosition);
        documentsStream.seek(documentsByteFloor + skipPositions.readInt());
        countsStream.seek(countsByteFloor + skipPositions.readInt());
        positionsStream.seek(positionsByteFloor + skipPositions.readLong());
      }
      documentIndex = (int) (skipDistance * skipsRead) - 1;
    }

    public boolean isDone() {
      return documentIndex >= documentCount;
    }

    public ExtentArray getData() {
      return extentArray;
    }

    public ExtentArray extents() {
      return extentArray;
    }

    public int currentCandidate() {
      return currentDocument;
    }

    public int count() {
      return currentCount;
    }

    public long totalEntries() {
      return ((long) documentCount);
    }

    public long totalPositions() {
      return totalPositionCount;
    }

    public DocumentContext getContext() {
      return this.context;
    }

    // This will pass up topdocs information if it's available
    public void setContext(DocumentContext context) {
	if ((context != null) && TopDocsContext.class.isAssignableFrom(context.getClass()) &&
              this.hasModifier("topdocs")) {
        ((TopDocsContext)context).hold = ((ArrayList<TopDocument>) getModifier("topdocs"));
        // remove the pointer to the mod (don't need it anymore)
        this.modifiers.remove("topdocs");
      }
      this.context = context;
    }
  }

  /**
   * This iterator simply ignores the positions information - faster b/c when incrementing or loading or skipping,
   * we don't have to bookkeep the positions buffer. Overall smaller footprint and faster execution.
   *
   */
  public class TermCountIterator extends KeyListReader.ListIterator
      implements AggregateIterator, CountValueIterator, ContextualIterator {

    DocumentContext context;
    int documentCount;
    int collectionCount;
    VByteInput documents;
    VByteInput counts;
    int documentIndex;
    int currentDocument;
    int currentCount;
    // Support for resets
    long startPosition, endPosition;
    RandomAccessFile input;
    // to support skipping
    VByteInput skips;
    VByteInput skipPositions;
    DataStream skipPositionsStream;
    DataStream documentsStream;
    DataStream countsStream;
    int skipDistance;
    int skipResetDistance;
    long numSkips;
    long skipsRead;
    long nextSkipDocument;
    long lastSkipPosition;
    long documentsByteFloor;
    long countsByteFloor;

    TermCountIterator(GenericIndexReader.Iterator iterator) throws IOException {
      reset(iterator);
    }

    // Initialization method.
    //
    // Even though we check for skips multiple times, in terms of how the data is loaded
    // its easier to do the parts when appropriate
    protected void initialize() throws IOException {
      input.seek(startPosition);
      DataInput stream = new VByteInput(input);

      // metadata
      int options = stream.readInt();
      documentCount = stream.readInt();
      collectionCount = stream.readInt();
      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipDistance = stream.readInt();
        skipResetDistance = stream.readInt();
        numSkips = stream.readLong();
      }

      // segment lengths
      long documentByteLength = stream.readLong();
      long countsByteLength = stream.readLong();
      long positionsByteLength = stream.readLong();
      long skipsByteLength = 0;
      long skipPositionsByteLength = 0;

      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipsByteLength = stream.readLong();
        skipPositionsByteLength = stream.readLong();
      }

      long documentStart = input.getFilePointer();
      long documentEnd = documentStart + documentByteLength;

      long countsStart = documentEnd;
      long countsEnd = countsStart + countsByteLength;

      // Still do this math to ensure correctness in the assertion below
      long positionsStart = countsEnd;
      long positionsEnd = positionsStart + positionsByteLength;


      if ((options & HAS_SKIPS) == HAS_SKIPS) {

        long skipsStart = positionsEnd;
        long skipsEnd = skipsStart + skipsByteLength;

        long skipPositionsStart = skipsEnd;
        long skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;

        assert skipPositionsEnd == endPosition;

        // we do these here b/c of scoping issues w/ the variables above
        documentsStream = new BufferedFileDataStream(input, documentStart, documentEnd);
        documents = new VByteInput(documentsStream);
        countsStream = new BufferedFileDataStream(input, countsStart, countsEnd);
        counts = new VByteInput(countsStream);
        skips = new VByteInput(new BufferedFileDataStream(input, skipsStart, skipsEnd));
        skipPositionsStream = new BufferedFileDataStream(input, skipPositionsStart,
                skipPositionsEnd);
        skipPositions = new VByteInput(skipPositionsStream);

        // load up
        nextSkipDocument = skips.readInt();
        documentsByteFloor = 0;
        countsByteFloor = 0;
      } else {
        assert positionsEnd == endPosition;
        skips = null;
        skipPositions = null;
        documents = new VByteInput(new BufferedFileDataStream(input, documentStart, documentEnd));
        counts = new VByteInput(new BufferedFileDataStream(input, countsStart, countsEnd));
      }
      documentIndex = 0;

      load();
    }

    // Only loading the docid and the count
    private void load() throws IOException {
      currentDocument += documents.readInt();
      currentCount = counts.readInt();
    }

    public String getEntry() {
      StringBuilder builder = new StringBuilder();

      builder.append(getKey());
      builder.append(",");
      builder.append(currentDocument);
      builder.append(",");
      builder.append(currentCount);

      return builder.toString();
    }

    public void reset(GenericIndexReader.Iterator iterator) throws IOException {
      startPosition = iterator.getValueStart();
      endPosition = iterator.getValueEnd();
      dataLength = iterator.getValueLength();
      input = iterator.getInput();
      key = iterator.getKey();
      initialize();
    }

    public void reset() throws IOException {
      currentDocument = 0;
      currentCount = 0;
      initialize();
    }

    public boolean next() throws IOException {
      documentIndex = Math.min(documentIndex + 1, documentCount);
      if (!isDone()) {
        load();
        return true;
      }
      return false;
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
        repositionMainStreams();
      }

      // linear from here
      while (document > currentDocument && next());
      return hasMatch(document);
    }

    // This only moves forward in tier 1, reads from tier 2 only when
    // needed to update floors
    //
    private void skipOnce() throws IOException {
      assert skipsRead < numSkips;
      long currentSkipPosition = lastSkipPosition + skips.readInt();

      if (skipsRead % skipResetDistance == 0) {
        // Position the skip positions stream
        skipPositionsStream.seek(currentSkipPosition);

        // now set the floor values
        documentsByteFloor = skipPositions.readInt();
        countsByteFloor = skipPositions.readInt();
        skipPositions.readLong(); // throw away, but we have to move it forward
      }
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

    private void repositionMainStreams() throws IOException {
      // If we just reset the floors, don't read the 2nd tier again
      if ((skipsRead - 1) % skipResetDistance == 0) {
        documentsStream.seek(documentsByteFloor);
        countsStream.seek(countsByteFloor);
      } else {
        skipPositionsStream.seek(lastSkipPosition);
        documentsStream.seek(documentsByteFloor + skipPositions.readInt());
        countsStream.seek(countsByteFloor + skipPositions.readInt());
        // we seek here, so no reading needed
      }
      documentIndex = (int) (skipDistance * skipsRead) - 1;
    }

    public boolean isDone() {
      return documentIndex >= documentCount;
    }

    public int currentCandidate() {
      return currentDocument;
    }

    public int count() {
      return currentCount;
    }

    public long totalEntries() {
      return documentCount;
    }

    // TODO: Declare in an interface
    public long totalPositions() {
      return collectionCount;
    }

    public DocumentContext getContext() {
	return this.context;
    }

    // This will pass up topdocs information if it's available
    public void setContext(DocumentContext context) {
	if ((context != null) && TopDocsContext.class.isAssignableFrom(context.getClass()) &&
              this.hasModifier("topdocs")) {
        ((TopDocsContext)context).hold = ((ArrayList<TopDocument>) getModifier("topdocs"));
        // remove the pointer to the mod (don't need it anymore)
        this.modifiers.remove("topdocs");
      }
      this.context = context;
    }
  }

  public PositionIndexReader(GenericIndexReader reader) throws IOException {
    super(reader);
  }

  public PositionIndexReader(String pathname) throws FileNotFoundException, IOException {
    super(pathname);
  }

  public Parameters getManifest() {
    return reader.getManifest();
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  /**
   * Returns an iterator pointing at the specified term, or
   * null if the term doesn't exist in the inverted file.
   */
  public TermExtentIterator getTermExtents(String term) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));
    if (iterator != null) {
      return new TermExtentIterator(iterator);
    }
    return null;
  }

  public TermCountIterator getTermCounts(String term) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));

    if (iterator != null) {
      return new TermCountIterator(iterator);
    }
    return null;
  }

  public void close() throws IOException {
    reader.close();
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(TermCountIterator.class));
    types.put("extents", new NodeType(TermExtentIterator.class));
    return types;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("counts")) {
      return getTermCounts(node.getDefaultParameter("term"));
    } else {
      return getTermExtents(node.getDefaultParameter("term"));
    }
  }

  // I add these in order to return intID frequency and collection frequency
  // information for terms. Any other way from the iterators are SLOW
  // unless the headers have already been loaded.
  // We need a better interface for these.
  // TODO:: Clean abstraction for this
  public long documentCount(String term) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));
    if (iterator == null) {
      return 0;
    }

    long startPosition = iterator.getValueStart();
    long endPosition = iterator.getValueEnd();

    RandomAccessFile input = iterator.getInput();
    input.seek(startPosition);
    DataInput stream = new VByteInput(input);

    // header information - have to read b/c it's compressed
    stream.readInt(); // skip option information
    int documentCount = stream.readInt();
    return documentCount;
  }

  // TODO: Clean abstraction for this
  public long termCount(String term) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));
    if (iterator == null) {
      return 0;
    }
    long startPosition = iterator.getValueStart();
    long endPosition = iterator.getValueEnd();

    RandomAccessFile input = iterator.getInput();
    input.seek(startPosition);
    DataInput stream = new VByteInput(input);

    // Can't just seek b/c the numbers are compressed
    stream.readInt();
    stream.readInt();
    int totalPositionCount = stream.readInt();
    return totalPositionCount;
  }
}
