package org.galagosearch.core.index;

import java.io.IOException;
import org.galagosearch.core.retrieval.structured.StructuredIterator;

/**
 *
 * @author irmarc, sjh
 */
public interface ValueIterator extends BoundedIterator, Comparable<ValueIterator> {

  int currentCandidate();

  boolean hasMatch(int identifier);

  boolean next() throws IOException;

  boolean moveTo(int identifier) throws IOException;

  void movePast(int identifier) throws IOException;

  String getEntry() throws IOException;
}
