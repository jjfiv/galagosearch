// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
public class MemoryPostings implements StructuredIndexPartReader{

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

    public Iterator( java.util.Iterator<byte[]> iterator  ) throws IOException{
      keyIterator = iterator;
      key = keyIterator.next();

      load();
    }

    // initialize for current key
    public void load(){ 
      postingList = postings.get(key);
      documentCount = postingList.documents.getPosition();
      extentArray = new ExtentArray();
      documentIndex = 0;
      positionIndex = 0;

      loadExtents();
    }

    // load extents for the current document
    public void loadExtents(){
      currentDocument = postingList.documents.getBuffer()[documentIndex];
      currentCount = postingList.counts.getBuffer()[documentIndex];
      extentArray.reset();
      
      for(int i = 0 ; i < currentCount ; i++){
        int position = postingList.positions.getBuffer()[positionIndex + i];
        extentArray.add(currentDocument, position, position + 1);
      }
      positionIndex += currentCount;
      // positionIndex should now point to the first term position of the next document
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
      if (!isDone()) return true;
      if (keyIterator.hasNext()){
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
    	keyIterator = postings.navigableKeySet().tailSet(key , true).iterator();
    	if(keyIterator.next().equals(key)) {
    		key=inkey;
    		load();
    		return true;
    	}
    	return false;
    }
    
    public String getKey() {
      return Utility.toString(key);
    }
    
    public byte[] getKeyBytes(){
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
    IntArray documents = new IntArray();
    IntArray counts = new IntArray();
    IntArray positions = new IntArray();

    int currentDocumentPosition;

    public PostingList(int document){
      documents.add(document);
      counts.add(0);
      currentDocumentPosition = 0;
    }

    public void add(int document, int position){
      if(documents.getBuffer()[currentDocumentPosition] == document){
        counts.getBuffer()[currentDocumentPosition] += 1;
      } else {
        currentDocumentPosition += 1;
        documents.add(document);
        counts.add(1);
      }
      positions.add(position);
    }
  }
  
  private class ByteArrComparator implements Comparator<byte[]> {
	  public int compare(byte[] a, byte[] b){
		  return Utility.compare(a,b);
	  }
  }
  
  // this could be a bit big -- but we need random access here
  // should use a trie (but java doesn't have one?)
  private TreeMap<byte[], PostingList> postings = new TreeMap(new ByteArrComparator());

  public void addPosting(String word, int document, int position){
	byte[] byteWord = Utility.fromString(word);
    if(postings.containsKey(byteWord)){
      PostingList postingList = postings.get(byteWord);
      postingList.add(document, position);
    } else {
      PostingList postingList = new PostingList(document);
      postingList.add(document, position);
      postings.put(byteWord, postingList);
    }
  }

  public void addPosting(NumberWordPosition nwp) {
    this.addPosting(Utility.toString(nwp.word), nwp.document, nwp.position);
  }


  public Iterator getIterator() throws IOException{
    return new Iterator( postings.keySet().iterator() );
  }
  public Iterator getIterator(String term) throws IOException{
    return new Iterator( postings.tailMap(Utility.fromString(term)).keySet().iterator());
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

