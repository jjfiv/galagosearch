// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Comparator;

import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.retrieval.structured.ExtentIndexIterator;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.ExtentIterator;
import org.galagosearch.core.retrieval.structured.IndexIterator;
import org.galagosearch.core.types.NumberWordPosition;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.core.util.IntArray;
import org.galagosearch.tupleflow.Utility;


/*
 * author sjh
 *
 * In-memory index - posting list component
 *
 */
public class MemoryPostings implements StructuredIndexPartReader {

  // iterator allows for query processing and for streaming posting list data
  //  public class Iterator extends ExtentIterator implements IndexIterator {
  public class Iterator extends ExtentIndexIterator {

    java.util.Iterator<byte[]> keyIterator;
    byte[] key;
    PostingList postingList;
    int documentIndex; // index in postingList structure
    int positionIndex; // index in postingList structure
    int documentCount; // count of documents in postingList structure
    int currentDocument; // current document number
    int currentCount; // number of repetitions of key in the current document
    ExtentArray extentArray; // container for positions in current document

    /**
     * Iterator over the entire index
     *
     * @param key
     * @throws IOException
     */
    public Iterator(java.util.Iterator<byte[]> iterator) throws IOException {
      keyIterator = iterator;
      key = keyIterator.next();
      load();
    }

    /**
     * Define an iterator over a single key
     *
     * @param key
     * @throws IOException
     */
    public Iterator(byte[] key) throws IOException {
      this.key = key;

      load();
    }

    // initialize for current key
    public void load() {
      postingList = postings.get(key);
      if (postingList == null) {
        documentCount = 0;
      } else {
        documentCount = postingList.documents.getPosition();
      }
      extentArray = new ExtentArray();
      documentIndex = 0;
      positionIndex = 0;

      loadExtents();
    }

    // load extents for the current document
    public void loadExtents() {
      if (postingList != null) {
        currentDocument = postingList.documents.getBuffer()[documentIndex];
        currentCount = postingList.termFreqCounts.getBuffer()[documentIndex];
        extentArray.reset();

        for (int i = 0; i < currentCount; i++) {
          int position = postingList.termPositions.getBuffer()[positionIndex + i];
          extentArray.add(currentDocument, position, position + 1);
        }
        positionIndex += currentCount;
        // positionIndex should now point to the first term position of the next document
      }
    }

    // restart from the current key
    public void reset() throws IOException {
      load();
    }

    public String getRecordString() {
      StringBuilder builder = new StringBuilder();

      builder.append(key);
      builder.append(",");
      builder.append(currentDocument);
      for (int i = 0; i < extentArray.getPositionCount(); ++i) {
        builder.append(",");
        builder.append(extentArray.getBuffer()[i].begin);
      }

      return builder.toString();
    }

    public boolean nextRecord() throws IOException {
      nextEntry();
      if (!isDone()) {
        return true;
      }
      if (keyIterator.hasNext()) {
        key = keyIterator.next();
        reset();
        return true;
      }
      return false;
    }

    public void nextEntry() throws IOException {
      documentIndex++;
      if (!isDone()) {
        loadExtents();
      }
    }

    public boolean skipTo(byte[] inkey) {
      keyIterator = postings.navigableKeySet().tailSet(key, true).iterator();
      if (keyIterator.next().equals(key)) {
        key = inkey;
        load();
        return true;
      }
      return false;
    }

    public String getKey() {
      return Utility.toString(key);
    }

    public byte[] getKeyBytes() {
      return key;
    }

    public int count() {
      return currentCount;
    }

    public int document() {
      return currentDocument;
    }

    public ExtentArray extents() {
      return extentArray;
    }

    public boolean isDone() {
      return documentIndex >= documentCount;
    }
  }

  public class PostingList {

    private final byte[] m_termBytes;
    IntArray documents = new IntArray();
    IntArray termFreqCounts = new IntArray();
    IntArray termPositions = new IntArray();
    int numDocs = 0;

    public PostingList(byte[] termBytes) {
      m_termBytes = termBytes;
    }

    public void add(int document, int position) {

      if (numDocs == 0) {
        // first doc added to posting list
        numDocs++;
        documents.add(document);
        termFreqCounts.add(1);
      } else {

        if (documents.getBuffer()[numDocs - 1] == document) {
          // add an occurrence in an existing document in the posting list
          termFreqCounts.getBuffer()[numDocs - 1] += 1;
        } else {

          // start a new document in the posting list;
          numDocs++;
          documents.add(document);
          termFreqCounts.add(1);
        }

      }

      termPositions.add(position);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("term: " + Utility.toString(m_termBytes) + " num docs: " + numDocs + "; docs:");

      int posIdx = 0;
      int[] termPos = termPositions.getBuffer();
      for (int i = 0; i < numDocs; i++) {
        int termFreq = termFreqCounts.getBuffer()[i];
        StringBuilder pos = new StringBuilder();
        pos.append("[");
        for (int j = 0; j < termFreq; j++) {
          pos.append(termPos[posIdx] + ",");
          posIdx++;
        }
        pos.deleteCharAt(pos.length() - 1);
        pos.append("]");
        sb.append("(" + documents.getBuffer()[i] + "," + termFreq + "," + pos.toString() + ")");
      }
      return sb.toString();
    }
  }

  private class ByteArrComparator implements Comparator<byte[]> {

    public int compare(byte[] a, byte[] b) {
      return Utility.compare(a, b);
    }
  }
  // this could be a bit big -- but we need random access here
  // should use a trie (but java doesn't have one?)
  private TreeMap<byte[], PostingList> postings = new TreeMap(new ByteArrComparator());

  public void addPosting(byte[] byteWord, int document, int position) {
    if (postings.containsKey(byteWord)) {
      PostingList postingList = postings.get(byteWord);
      postingList.add(document, position);
    } else {
      PostingList postingList = new PostingList(byteWord);
      postingList.add(document, position);
      postings.put(byteWord, postingList);
    }
  }

  public void addPosting(NumberWordPosition nwp) {
    this.addPosting(nwp.word, nwp.document, nwp.position);
  }

  public Iterator getIterator() throws IOException {
    return new Iterator(postings.keySet().iterator());
  }

  public Iterator getIterator(String term) throws IOException {
    return new Iterator(Utility.fromString(term));
  }

  public IndexIterator getIterator(Node node) throws IOException {
    return getIterator(node.getDefaultParameter("term"));
  }

  public void close() throws IOException {
    // Do Nothing -- could reduce RAM usage;
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(Iterator.class));
    types.put("extents", new NodeType(Iterator.class));
    return types;
  }
}
