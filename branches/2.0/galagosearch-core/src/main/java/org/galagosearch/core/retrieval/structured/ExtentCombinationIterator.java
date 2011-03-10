/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.ArrayList;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.util.ExtentArray;
import org.galagosearch.tupleflow.Utility;

/**
 * Abstract class to provide some of the higher-level navigation and logic based on implementation carried out
 * in any implementing subclass. Any iterator that operates on multiple extent iterators should subclass this class.
 *
 * @author irmarc
 */
public abstract class ExtentCombinationIterator implements ExtentValueIterator, CountValueIterator {

  /**
   * The iterators this iterator manages.
   */
  protected ExtentValueIterator[] iterators;
  /**
   * The currently loaded set of extents. Cannot be null, but can be empty.
   */
  protected ExtentArray extents;

  /**
   * Returns a string representation of the currently loaded extents.
   * @return
   * @throws IOException
   */
  public String getEntry() throws IOException {
    Extent[] exts = extents.toArray();
    ArrayList<String> strs = new ArrayList<String>();
    for (Extent e : exts) {
      strs.add(e.toString());
    }
    return Utility.join(strs.toArray(new String[0]), ",");
  }

  /**
   * Return the currently loaded extents.
   */
  public ExtentArray extents() {
    return extents;
  }

  /**
   * Return the currently loaded extents.
   */
  public ExtentArray getData() {
    return extents;
  }

  /**
   * Returns the number of extents in the current document. Note that this is
   * <i>not</i> the same as the number of positions found in the document.
   */
  public int count() {
    return extents().getPositionCount();
  }

  /**
   * Returns whether or not this iterator is currently evaluating the identifier passed.
   */
  public boolean hasMatch(int identifier) {
    return (currentIdentifier() == identifier);
  }

  /**
   * Moves this iterator past the identifier passed in.
   * @param identifier
   * @throws IOException
   */
  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  /**
   * The comparison first performs a "done" check on both iterators. Any iterator that is done is
   * considered to have the highest possible value according to ordering. If neither iterator is done,
   * then the comparison is a standard integer compare based on the current identifiers being evaluated
   * by the two iterators.
   * @param other
   * @return
   */
  public int compareTo(ValueIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return currentIdentifier() - other.currentIdentifier();
  }

  /**
   * This forms the core policy of the iterator. Which extents get loaded ultimately determine the
   * behavior of the other functions. See #OrderedWindowIterator or #ExtentInsideIterator for some good examples.
   */
  public abstract void loadExtents();
}
