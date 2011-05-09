// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.IOException;

public interface AggregateReader {
    public int documentCount(String term) throws IOException;
    public int termCount(String term) throws IOException;
}