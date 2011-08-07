// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import junit.framework.TestCase;
import org.galagosearch.core.index.GenericIndexReader;
import org.galagosearch.core.index.ValueIterator;

import org.galagosearch.core.index.corpus.CorpusReader;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.index.corpus.DocumentIndexReader;
import org.galagosearch.core.index.IndexReader;
import org.galagosearch.core.index.corpus.DocumentReader;
import org.galagosearch.core.index.corpus.DocumentReader.DocumentIterator;
import org.galagosearch.core.retrieval.query.Node;
import org.galagosearch.core.retrieval.query.NodeType;
import org.galagosearch.tupleflow.Parameters;

public class DocumentIndexStoreTest extends TestCase {

  public DocumentIndexStoreTest(String testName) {
    super(testName);
  }

  class MockReader implements DocumentReader {

    public boolean closeCalled = false;

    public Document getDocument(String identifier) {
      Document document = new Document();
      document.text = "hi";
      document.identifier = "id";
      return document;
    }

    public void close() {
      closeCalled = true;
    }

    public DocumentIterator getIterator() throws IOException {
      throw new UnsupportedOperationException("unsupported");
    }

    public Map<String, NodeType> getNodeTypes() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public ValueIterator getIterator(Node node) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getDefaultOperator() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    public Parameters getManifest() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public void testStore() throws IOException {
    MockReader reader = new MockReader();
    DocumentIndexStore store = new DocumentIndexStore(reader);

    Document result = store.get("something");
    assertEquals("hi", result.text);
    assertEquals("id", result.identifier);
    store.close();
    assertTrue(reader.closeCalled);
  }
}
