// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import gnu.trove.TIntHashSet;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.galagosearch.core.index.corpus.SplitIndexValueWriter;
import org.galagosearch.core.index.merge.PositionIndexMerger;
import org.galagosearch.core.types.KeyValuePair;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.tupleflow.IncompatibleProcessorException;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Source;
import org.galagosearch.tupleflow.Step;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.TypeReader;
import org.galagosearch.tupleflow.Utility;
import org.galagosearch.tupleflow.execution.ErrorHandler;
import org.galagosearch.tupleflow.execution.Verification;
import org.galagosearch.tupleflow.types.XMLFragment;

/**
 * 12/14/2010 (irmarc): Adding a skip list to this structure. It's pretty
 * basic - we have a predefined skip distance in terms of how many entries to
 * skip. A skip is a two-tier structure:
 *
 * 1st tier: [d-gap doc id, d-gap byte offset to tier 2]
 * 2nd tier: [docs byte pos, counts byte pos, positions byte pos]
 *
 * Documents are d-gapped, but we already have those in tier 1. Counts are not d-gapped b/c
 * they store the # of positions, so they don't monotonically track. Positions are self-contained
 * (reset at a new doc boundary), so we only need the byte information in tier 2.
 *
 * Some variable names:
 * skipDistance: the maximum number of documents we store generating a skip.
 * skipResetDisance: the number of skips we generate before we reset the offset
 * base. Instead of storing the absolute values in the 2nd tier, all entries that are
 * some factor x*skipResetDistance are absolute values, and all values until (x+1)*skipResetDistance
 * entries away are d-gapped off that absolute value so there are a few extra reads (or if you're clever
 * only one extra read), but it keeps the 2nd tier values from ballooning fast, and we don't need to
 * read them all in order to recover the original values.
 *
 * @author trevor, irmarc
 */
@InputClass(className = "org.galagosearch.core.types.NumberWordPosition", order = {"+word", "+document", "+position"})
@OutputClass(className = "org.galagosearch.core.types.KeyValuePair", order = {"+key"})
public class PositionIndexWriter implements
        NumberWordPosition.WordDocumentPositionOrder.ShreddedProcessor,
        Source<KeyValuePair> // parallel index data output
{

  public class PositionsList implements IndexElement {

    public PositionsList() {
      documents = new CompressedRawByteBuffer();
      counts = new CompressedRawByteBuffer();
      positions = new CompressedRawByteBuffer();
      header = new CompressedByteBuffer();

      if ((options & KeyListReader.ListIterator.HAS_SKIPS) == KeyListReader.ListIterator.HAS_SKIPS) {
        skips = new CompressedRawByteBuffer();
        skipPositions = new CompressedRawByteBuffer();
      } else {
        skips = null;
      }
    }

    public void close() throws IOException {

      if (documents.length() > 0) {
        counts.add(positionCount);
      }

      if (skips != null && skips.length() == 0) {
        // not adding skip information b/c its empty
        options &= (0xffff - KeyListReader.ListIterator.HAS_SKIPS);
        header.add(options);
      } else {
        header.add(options);
      }

      header.add(documentCount);
      header.add(totalPositionCount);

      if (skips != null && skips.length() > 0) {
        header.add(skipDistance);
        header.add(skipResetDistance);
        header.add(numSkips);
      }

      header.add(documents.length());
      header.add(counts.length());
      header.add(positions.length());
      if (skips != null && skips.length() > 0) {
        header.add(skips.length());
        header.add(skipPositions.length());
      }
    }

    public long dataLength() {
      long listLength = 0;

      listLength += header.length();
      listLength += counts.length();
      listLength += positions.length();
      listLength += documents.length();
      if (skips != null) {
        listLength += skips.length();
        listLength += skipPositions.length();
      }

      return listLength;
    }

    public void write(final OutputStream output) throws IOException {
      header.write(output);
      header.clear();

      documents.write(output);
      documents.clear();

      counts.write(output);
      counts.clear();

      positions.write(output);
      positions.clear();

      if (skips != null && skips.length() > 0) {
        skips.write(output);
        skips.clear();
        skipPositions.write(output);
        skipPositions.clear();
      }
    }

    public byte[] key() {
      return word;
    }

    public void setWord(byte[] word) {
      this.word = word;
      this.lastDocument = 0;
      this.lastPosition = 0;
      this.totalPositionCount = 0;
      this.positionCount = 0;
      if (skips != null) {
        this.docsSinceLastSkip = 0;
        this.lastSkipPosition = 0;
        this.lastDocumentSkipped = 0;
        this.lastDocumentSkip = 0;
        this.lastCountSkip = 0;
        this.lastPositionSkip = 0;
        this.numSkips = 0;
      }

    }

    public void addDocument(long documentID) throws IOException {
      // add the last document's counts
      if (documents.length() > 0) {
        counts.add(positionCount);

        // if we're skipping check that
        if (skips != null) {
          updateSkipInformation();
        }
      }
      documents.add(documentID - lastDocument);
      lastDocument = documentID;

      lastPosition = 0;
      positionCount = 0;
      documentCount++;

    }

    public void addPosition(int position) throws IOException {
      positionCount++;
      totalPositionCount++;
      positions.add(position - lastPosition);
      lastPosition = position;
    }

    private void updateSkipInformation() {
      // There are already docs entered and we've gone skipDistance docs -- make a skip
      docsSinceLastSkip = (docsSinceLastSkip + 1) % skipDistance;
      if (documents.length() > 0 && docsSinceLastSkip == 0) {
        skips.add(lastDocument - lastDocumentSkipped);
        skips.add(skipPositions.length() - lastSkipPosition);
        lastDocumentSkipped = lastDocument;
        lastSkipPosition = skipPositions.length();

        // Now we decide whether we're storing an abs. value d-gapped value
        if (numSkips % skipResetDistance == 0) {
          // absolute values
          skipPositions.add(documents.length());
          skipPositions.add(counts.length());
          skipPositions.add(positions.length());
          lastDocumentSkip = documents.length();
          lastCountSkip = counts.length();
          lastPositionSkip = positions.length();
        } else {
          // d-gap skip
          skipPositions.add(documents.length() - lastDocumentSkip);
          skipPositions.add(counts.length() - lastCountSkip);
          skipPositions.add((long) (positions.length() - lastPositionSkip));
        }
        numSkips++;
      }
    }
    private long lastDocument;
    private int lastPosition;
    private int positionCount;
    private int documentCount;
    private int totalPositionCount;
    public byte[] word;
    public CompressedByteBuffer header;
    public CompressedRawByteBuffer documents;
    public CompressedRawByteBuffer counts;
    public CompressedRawByteBuffer positions;
    // to support skipping
    private long lastDocumentSkipped;
    private long lastSkipPosition;
    private long lastDocumentSkip;
    private long lastCountSkip;
    private long lastPositionSkip;
    private long numSkips;
    private long lastCount;
    private int docsSinceLastSkip;
    private CompressedRawByteBuffer skips;
    private CompressedRawByteBuffer skipPositions;
  }
  long maximumDocumentCount = 0;
  long maximumDocumentNumber = 0;
  PositionsList invertedList;
  DataOutputStream output;
  long filePosition;
  GenericIndexWriter writer;
  long documentCount = 0;
  long collectionLength = 0;
  int options = 0;
  int skipDistance;
  int skipResetDistance;
  byte[] lastWord;
  boolean hasStats;
  TIntHashSet uniqueDocs;
  // parallel index stuff
  boolean parallel;

  /**
   * Creates a new instance of PositionIndexWriter
   */
  public PositionIndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    Parameters actualParams = parameters.getXML();
    actualParams.add("writerClass", getClass().getName());
    actualParams.add("mergerClass", PositionIndexMerger.class.getName());
    actualParams.add("readerClass", PositionIndexReader.class.getName());
    actualParams.add("defaultOperator", "counts");

    // Let's get those XMLFragments in there if we're receiving
    if (actualParams.containsKey("pipename")) {
      TypeReader<XMLFragment> collectionStats = parameters.getTypeReader(actualParams.get("pipename"));
      XMLFragment frag;
      while ((frag = collectionStats.read()) != null) {
        actualParams.add("statistics/" + frag.nodePath, frag.innerText);
      }
      hasStats = true;
    } else {
      hasStats = false;
      uniqueDocs = new TIntHashSet();
    }

    writer = new IndexWriter(parameters);

    // look for skips
    boolean skip = Boolean.parseBoolean(parameters.getXML().get("skipping", "true"));
    skipDistance = (int) parameters.getXML().get("skipDistance", 500);
    skipResetDistance = (int) parameters.getXML().get("skipResetDistance", 20);
    options |= (skip ? KeyListReader.ListIterator.HAS_SKIPS : 0x0);
    // more options here?
  }

  public void processWord(byte[] wordBytes) throws IOException {
    if (invertedList != null) {
      if (!hasStats) {
        collectionLength += invertedList.totalPositionCount;
      }
      invertedList.close();
      writer.add(invertedList);

      invertedList = null;
    }

    resetDocumentCount();

    invertedList = new PositionsList();
    invertedList.setWord(wordBytes);
    if (wordBytes.length > 255) {
      System.err.printf("TOO LONG (%d): %s\n", wordBytes.length, Utility.toString(wordBytes));
    }
    assert lastWord == null || 0 != Utility.compare(lastWord, wordBytes) : "Duplicate word";
    lastWord = wordBytes;
  }

  public void processDocument(int document) throws IOException {
    invertedList.addDocument(document);
    documentCount++;
    if (!hasStats) {
      uniqueDocs.add(document);
    }
    maximumDocumentNumber = Math.max(document, maximumDocumentNumber);
  }

  public void processPosition(int position) throws IOException {
    invertedList.addPosition(position);
  }

  public void processTuple() {
    // does nothing
  }

  private void resetDocumentCount() {
    maximumDocumentCount = Math.max(documentCount, maximumDocumentCount);
    documentCount = 0;
  }

  public void close() throws IOException {
    if (invertedList != null) {
      if (!hasStats) {
        collectionLength += invertedList.totalPositionCount;
      }
      invertedList.close();
      writer.add(invertedList);
    }

    // Add stats to the manifest if needed
    if (!hasStats) {
      Parameters manifest = writer.getManifest();
      if (!manifest.containsKey("statistics/documentCount")) {
        manifest.add("statistics/documentCount", Long.toString(uniqueDocs.size()));
      }
      if (!manifest.containsKey("statistics/collectionLength")) {
        manifest.add("statistics/collectionLength", Long.toString(collectionLength));
      }
    }

    writer.close();
  }

  public long documentCount() {
    return maximumDocumentNumber;
  }

  public long maximumDocumentCount() {
    return maximumDocumentCount;
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getXML().containsKey("filename")) {
      handler.addError("PositionIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getXML().get("filename");
    Verification.requireWriteableFile(index, handler);
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    writer.setProcessor(processor);
  }
}
