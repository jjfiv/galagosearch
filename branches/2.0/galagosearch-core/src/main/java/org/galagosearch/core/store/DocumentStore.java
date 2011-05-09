// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.store;

import java.io.IOException;
import org.galagosearch.core.parse.Document;

/**
 * This interface abstracts a document collection.  The SnippetGenerator uses
 * this interface to fetch documents for snippeting.
 * 
 * @author trevor
 */
public interface DocumentStore {
    Document get(String identifier) throws IOException;
    void close() throws IOException;
}
