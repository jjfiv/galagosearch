/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.galagosearch.core.index;

import java.io.IOException;
import org.galagosearch.core.retrieval.structured.StructuredIterator;

/**
 *
 * @author irmarc
 */
public interface ValueIterator extends StructuredIterator {
  public static final int HAS_SKIPS = 0x01;

  void reset(GenericIndexReader.Iterator it) throws IOException;

  long totalEntries() throws IOException;

  boolean skipToEntry(int id) throws IOException;

  boolean skipToEntry(long id) throws IOException;

  boolean skipToEntry(String id) throws IOException;

  boolean nextEntry() throws IOException;

  // This is for display purposes - otherwise we wouldn't even
  // try to determine the data type of the entry
  String getEntry() throws IOException;

  public boolean hasMatch(int id);

  public boolean hasMatch(long id);

  public boolean hasMatch(String id);

  public void moveTo(int id) throws IOException;

  public void moveTo(long id) throws IOException;

  public void moveTo(String id) throws IOException;

  public void movePast(int id) throws IOException;

  public void movePast(long id) throws IOException;

  public void movePast(String id) throws IOException;
}
