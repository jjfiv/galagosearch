/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import java.util.PriorityQueue;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.core.util.ExtentArray;

/**
 *
 * @author marc
 */
public abstract class ExtentCombinationIterator implements ExtentValueIterator, CountValueIterator {

  protected ExtentValueIterator[] iterators;
  protected ExtentArray extents;

  public ExtentArray extents() {
    return extents;
  }

  public ExtentArray getData() {
    return extents;
  }

  public int count() {
    return extents().getPositionCount();
  }

  public boolean hasMatch(int identifier) {
    return (currentIdentifier == identifier);
  }

  public void movePast(int identifier) throws IOException {
      moveTo(identifier+1);
  }

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

  public abstract void loadExtents();
}
