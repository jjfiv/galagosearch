/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.retrieval.structured;

import gnu.trove.TObjectDoubleHashMap;
import java.io.IOException;
import org.galagosearch.core.index.ValueIterator;

/**
 *
 * @author marc
 */
public abstract class TransformIterator implements ScoreValueIterator {

  DocumentContext context;
  ValueIterator iterator;

  public TransformIterator(ValueIterator iterator) {
    this.iterator = iterator;
  }

  public DocumentContext getContext() {
    return context;
  }

  public void setContext(DocumentContext context) {
    this.context = context;
  }

  public void reset() throws IOException {
    iterator.reset();
  }

  public boolean isDone() {
    return iterator.isDone();
  }

  public int currentIdentifier() {
    return iterator.currentIdentifier();
  }

  public boolean hasMatch(int identifier) {
    return iterator.hasMatch(identifier);
  }

  public boolean next() throws IOException {
    return iterator.next();
  }

  public boolean moveTo(int identifier) throws IOException {
    return iterator.moveTo(identifier);
  }

  public void movePast(int identifier) throws IOException {
    iterator.movePast(identifier);
  }

  public long totalEntries() {
    return iterator.totalEntries();
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
