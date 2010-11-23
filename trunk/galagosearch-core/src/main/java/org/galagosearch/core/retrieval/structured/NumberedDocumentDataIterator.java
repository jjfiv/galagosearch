// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

import org.galagosearch.core.types.NumberedDocumentData;

/**
 * A DocumentDataIterator allows a user to scan through
 * a documentLengths or documentNames index in some order
 * (eg: document number, or document name)
 *
 * This is useful when streaming data from an in memory
 * index to disk or when merging a set of indexes.
 *
 * Note that data will generally be incomplete.
 *
 * @author sjh
 */
public abstract class NumberedDocumentDataIterator implements IndexIterator {
  // useful for merging indexes (and ignored by all other classes)
  public int indexId = 0; 
  
  // Allows data to be retrieved
  public abstract NumberedDocumentData getDocumentData() throws IOException;
}
