// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index.mem;

import org.galagosearch.tupleflow.Parameters;

public class MemoryManifest {

    Long collectionLength = 0L;
    Long documentCount = 0L;
    int documentNumberOffset = 0;

    public void addDocument(int length){
      documentCount += 1;
      collectionLength += length;
    }

    public void setOffset(int offset){
      documentNumberOffset = offset;
    }
    
    public long getCollectionLength(){
      return collectionLength;
    }

    public long getDocumentCount(){
      return documentCount;
    }

    public int getOffset(){
      return documentNumberOffset;
    }

    public Parameters makeParameters() {
      Parameters p = new Parameters();
      p.add("collectionLength", Long.toString(collectionLength));
      p.add("documentCollection", Long.toString(documentCount));
      p.add("documentNumberOffset", Integer.toString(documentNumberOffset));
      return p;
    }
}
