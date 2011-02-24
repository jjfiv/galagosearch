/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 *
 * @author irmarc
 */
public abstract class NavigableIterator implements StructuredIterator {

  public boolean hasMatch(int id) {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public boolean hasMatch(long id) {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public boolean hasMatch(String id) {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public int intID() {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public long longID() {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public String stringID() {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public void movePast(int id) throws IOException {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public void movePast(long id) throws IOException {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public void movePast(String id) throws IOException {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public boolean moveTo(int id) throws IOException {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public boolean moveTo(long id) throws IOException {
    throw new UnsupportedOperationException("Not implemented here.");
  }

  public boolean moveTo(String id) throws IOException {
    throw new UnsupportedOperationException("Not implemented here.");
  }
}
