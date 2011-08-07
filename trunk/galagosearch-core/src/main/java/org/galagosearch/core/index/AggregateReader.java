// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.index;

import java.io.IOException;

public interface AggregateReader {

  public static interface AggregateIterator {

    public long totalEntries();

    public long totalPositions();
  }

  public long documentCount(String term) throws IOException;

  public long termCount(String term) throws IOException;
}
