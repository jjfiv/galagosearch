// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 * This is a marker interface that represents any kind of
 * iterator over an inverted list or query operator.
 * 
 * @author trevor
 */
public interface StructuredIterator {
    public void reset() throws IOException;
}
