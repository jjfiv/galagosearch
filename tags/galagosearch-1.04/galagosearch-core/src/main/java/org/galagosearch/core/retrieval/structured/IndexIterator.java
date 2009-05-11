// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 * Each iterator from an index has an extra two methods,
 * getRecordstring() and nextRecord(), that allows the data from
 * the index to be easily printed.  DumpIndex uses this functionality
 * to dump the contents of any Galago index.
 * 
 * @author trevor
 */
public interface IndexIterator extends StructuredIterator {
    String getRecordString();
    boolean nextRecord() throws IOException;
}
