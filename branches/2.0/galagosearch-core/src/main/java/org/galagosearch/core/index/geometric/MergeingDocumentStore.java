/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index.geometric;

import java.io.IOException;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.store.DocumentIndexStore;
import org.galagosearch.core.store.DocumentStore;
import org.galagosearch.core.store.NullStore;

/**
 * 
 * @author sjh
 */
class MergeingDocumentStore implements DocumentStore {
  
  DocumentStore docStore;
  
  public MergeingDocumentStore() {
    docStore = new NullStore();
  }

  public Document get(String identifier) throws IOException {
    return this.docStore.get(identifier);
  }

  public void close() throws IOException {
    this.docStore.close();
  }

  void setDocStore(DocumentIndexStore documentStore) {
    this.docStore = documentStore;
  }
  
}
