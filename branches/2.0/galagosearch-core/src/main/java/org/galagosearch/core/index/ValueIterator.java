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

  long totalEntries();

  int currentIdentifier();

  boolean hasMatch(int id);

  boolean nextEntry() throws IOException;

  // This is for display purposes - otherwise we wouldn't even
  // try to determine the data type of the entry
  String getEntry() throws IOException;
}
