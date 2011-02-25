// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.util.ExtentArray;

/**
 *
 * @author trevor
 */
public class NullExtentIterator implements ExtentIterator {

  ExtentArray array = new ExtentArray();

  public boolean nextEntry() {
    return false;
  }

  public boolean isDone() {
    return true;
  }

  public ExtentArray extents() {
    return array;
  }

  public int identifier() {
    return Integer.MAX_VALUE;
  }

  public int count() {
    return 0;
  }

  public void reset() {
    // do nothing
  }

  public ExtentArray getData() {
    return array;
  }

  public long totalEntries() {
    return 0;
  }

  public int currentIdentifier() {
    return Integer.MAX_VALUE;
  }

  public boolean hasMatch(int id) {
    return false;
  }

  public String getEntry() throws IOException {
    return "NULL";
  }
}
