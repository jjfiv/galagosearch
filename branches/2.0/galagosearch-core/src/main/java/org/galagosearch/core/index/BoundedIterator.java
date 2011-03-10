// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import org.galagosearch.core.retrieval.structured.StructuredIterator;

/**
 * 
 * @author irmarc
 */
public interface BoundedIterator extends StructuredIterator {
  public long totalEntries();
}
