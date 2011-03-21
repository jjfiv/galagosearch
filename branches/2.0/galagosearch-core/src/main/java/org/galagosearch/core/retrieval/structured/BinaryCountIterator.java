/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;
import org.galagosearch.core.index.ValueIterator;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author marc
 */
public class BinaryCountIterator implements CountValueIterator
{
  IndicatorIterator iterator;

  public BinaryCountIterator(Parameters p, IndicatorIterator i) {
    iterator = i;
  }


  public int count() {
    return iterator.getIndicatorStatus();
  }

  public void reset() throws IOException {
    iterator.reset();
  }

  public boolean isDone() {
    return iterator.isDone();
  }

  public int currentCandidate() {
    return iterator.currentCandidate();
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

  public String getEntry() throws IOException {
    return iterator.getEntry();
  }

  public long totalEntries() {
    return iterator.totalEntries();
  }

  public int compareTo(ValueIterator t) {
    return iterator.compareTo(t);
  }

}
