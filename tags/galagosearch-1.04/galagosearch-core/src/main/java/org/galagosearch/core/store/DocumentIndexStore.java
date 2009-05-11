// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.store;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.galagosearch.core.parse.Document;
import org.galagosearch.core.parse.DocumentIndexReader;

/**
 * <p>A DocumentStore that reads document data from corpus files.</p>
 * A DocumentIndexStore checks the corpus files sequentially until
 * it finds a matching document.</p>
 * @author trevor
 */
public class DocumentIndexStore implements DocumentStore {
    List<DocumentIndexReader> readers;

    public DocumentIndexStore(DocumentIndexReader reader) {
        this(Collections.singletonList(reader));
    }

    public DocumentIndexStore(List<DocumentIndexReader> readers) {
        this.readers = readers;
    }

    public Document get(String identifier) throws IOException {
        for (DocumentIndexReader reader : readers) {
            Document document = reader.getDocument(identifier);
            if (document != null) {
                return document;
            }
        }
        return null;
    }

    public void close() throws IOException {
        for (DocumentIndexReader reader : readers) {
            reader.close();
        }
    }
}
