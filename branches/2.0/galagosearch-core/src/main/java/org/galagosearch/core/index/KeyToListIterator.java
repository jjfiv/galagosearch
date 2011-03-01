/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index;

import java.io.IOException;
import org.galagosearch.tupleflow.Utility;

/**
 *
 * @author marc
 */
public abstract class KeyToListIterator implements ValueIterator {

  protected KeyIterator iterator;

  public KeyToListIterator(KeyIterator ki) {
    iterator = ki;
  }

  public boolean next() throws IOException {
    return iterator.nextKey();
  }

  public boolean moveTo(int identifier) throws IOException {
    return iterator.moveToKey(Utility.fromInt(identifier));
  }

  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  public void reset() throws IOException {
    iterator.reset();
  }

  public boolean isDone() {
    return iterator.isDone();
  }

  public int currentIdentifier() {
    try {
      return Utility.toInt(iterator.getKeyBytes());
    } catch (IOException ioe) {
      return Integer.MAX_VALUE;
    }
  }

  public boolean hasMatch(int identifier) {
    return (currentIdentifier() == identifier);
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
}
