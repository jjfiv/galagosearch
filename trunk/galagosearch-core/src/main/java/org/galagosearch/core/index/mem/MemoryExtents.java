// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Comparator;

import org.galagosearch.core.index.StructuredIndexPartReader;
import org.galagosearch.core.retrieval.structured.ExtentIndexIterator;
import org.galagosearch.core.parse.Tag;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.core.retrieval.structured.ExtentIterator;
import org.galagosearch.core.retrieval.structured.IndexIterator;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.core.util.IntArray;
import org.galagosearch.tupleflow.Utility;


/*
 * author sjh
 * 
 * In-memory index - posting list component
 * 
 */
public class MemoryExtents implements StructuredIndexPartReader{

  // iterator allows for query processing and for streaming posting list data
//  public class Iterator extends ExtentIterator implements IndexIterator {
  public class Iterator extends ExtentIndexIterator {
    java.util.Iterator<byte[]> keyIterator;
    byte[] key;

    ExtentList extentList;
    int documentIndex; // index in postingList structure 
    int tagIndex; // index in postingList structure
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
      extentList = extents.get(key);
      documentCount = extentList.documents.getPosition();
      extentArray = new ExtentArray();
      documentIndex = 0;
      tagIndex = 0;
      
      loadExtents();
    }

    // load extents for the current document
    public void loadExtents(){
      currentDocument = extentList.documents.getBuffer()[documentIndex];
      currentCount = extentList.counts.getBuffer()[documentIndex];
      extentArray.reset();
      
      for(int i = 0; i < currentCount ; i++){
        int begin = extentList.begins.getBuffer()[tagIndex + i];
        int end = extentList.ends.getBuffer()[tagIndex + i];
        extentArray.add(currentDocument, begin, end);
      }
      tagIndex += currentCount;

      // tagIndex should now point to the first begin/end pair of the next document
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
        builder.append(",(");
        builder.append(extentArray.getBuffer()[i].begin);
        builder.append(",");
        builder.append(extentArray.getBuffer()[i].end);
        builder.append(")");
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
    
    public boolean skipTo(byte[] newkey) {
      keyIterator = extents.tailMap(newkey).keySet().iterator();
      if (keyIterator.next().equals(newkey) ) {
        key = newkey;
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

  
  public class ExtentList {
    IntArray documents = new IntArray();
    IntArray counts = new IntArray();
    IntArray begins = new IntArray();
    IntArray ends = new IntArray();

    int currentDocumentPosition;

    public ExtentList(int document){
      documents.add(document);
      counts.add(0);
      currentDocumentPosition = 0;
    }

    public void add(int document, Tag tag){
      if(documents.getBuffer()[currentDocumentPosition] == document){
        counts.getBuffer()[currentDocumentPosition] += 1;
      } else {
        currentDocumentPosition += 1;
        documents.add(document);
        counts.add(1);
      }
      begins.add(tag.begin);
      ends.add(tag.end);
    }
  }
  
  private class ByteArrComparator implements Comparator<byte[]> {
	  public int compare(byte[] a, byte[] b){
		  return Utility.compare(a,b);
	  }
  }
  
  // this could be a bit big -- but we need random access here
  // should use a trie (but java doesn't have one?)
  private TreeMap<byte[], ExtentList> extents = new TreeMap(new ByteArrComparator());

  public void addDocumentExtent(String extentName, int document, Tag tag){
	byte[] byteExtentName = Utility.fromString(extentName);
    if(extents.containsKey(byteExtentName)){
      ExtentList extentList = extents.get(byteExtentName);
      extentList.add(document, tag);
    } else {
      ExtentList extentList = new ExtentList(document);
      extentList.add(document, tag);
      extents.put(byteExtentName, extentList);
    }
  }
  

  public Iterator getIterator() throws IOException{
    return new Iterator( extents.keySet().iterator() );
  }
  public Iterator getIterator(String term) throws IOException{
    return new Iterator( extents.tailMap(Utility.fromString(term)).keySet().iterator());
  }
  public IndexIterator getIterator(Node node) throws IOException {
    return getIterator(node.getDefaultParameter("term"));
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(Iterator.class));
    types.put("extents", new NodeType(Iterator.class));
    return types;
  }

  
  
  public void close() throws IOException {
    // Do Nothing -- however we could reduce RAM usage;
  }

}

