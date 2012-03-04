// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.store;

import java.io.IOException;
import org.galagosearch.core.parse.Document;

/**
 * A very simple DocumentStore when the original document contents are not available.
 *
 * @author trevor
 */
public class NullStore implements DocumentStore {
    public Document get(String identifier) throws IOException {
        Document document = new Document(identifier, "");
        return document;
    }

    public void close() throws IOException {
    }
}
