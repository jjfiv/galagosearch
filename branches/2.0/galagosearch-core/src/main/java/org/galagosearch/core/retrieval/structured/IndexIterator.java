// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 * Each iterator from an index has an extra two methods,
 * getRecordstring() and nextRecord(), that allows the data from
 * the index to be easily printed.  DumpIndex uses this functionality
 * to dump the contents of any Galago index.
 *
 * (12/21/2010, irmarc): Added the skipTo method to allow for better navigation.
 *
 * @author trevor, irmarc
 */
public interface IndexIterator extends StructuredIterator {
    String getRecordString();
    boolean skipTo(byte[] key) throws IOException;
    boolean nextRecord() throws IOException;
    String getKey() throws IOException;
    // sjh: i need this method to ensure key order is consistent when merging indexes
    byte[] getKeyBytes() throws IOException;
}
