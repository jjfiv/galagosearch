// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.index.ValueIterator;

/**
 *
 * @author marc, sjh
 */
public interface IndicatorIterator extends ContextualIterator, ValueIterator, StructuredIterator {
  public int getIndicatorStatus();
  public boolean getStatus();
  public boolean getStatus(int document);
}
