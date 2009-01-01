// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.store;

import java.io.IOException;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.DocumentIndexReader;

/**
 *
 * @author trevor
 */
public class DocumentIndexStore implements DocumentStore {
    DocumentIndexReader reader;

    public DocumentIndexStore(DocumentIndexReader reader) {
        this.reader = reader;
    }

    public Document get(String identifier) throws IOException {
        return reader.getDocument(identifier);
    }

    public void close() throws IOException {
        reader.close();
    }
}
