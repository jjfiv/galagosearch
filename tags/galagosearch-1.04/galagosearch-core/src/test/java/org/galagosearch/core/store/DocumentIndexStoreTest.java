// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.store;

import java.io.IOException;
import junit.framework.TestCase;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.DocumentIndexReader;
import org.galagosearch.core.index.IndexReader;

public class DocumentIndexStoreTest extends TestCase {
    
    public DocumentIndexStoreTest(String testName) {
        super(testName);
    }

    class MockReader extends DocumentIndexReader {
        public boolean closeCalled = false;
        MockReader() { super((IndexReader)null); }

        @Override
        public Document getDocument(String identifier) {
            Document document = new Document();
            document.text = "hi";
            document.identifier = "id";
            return document;
        }

        @Override
        public void close() {
            closeCalled = true;
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
