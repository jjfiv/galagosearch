// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 *
 * @author irmarc
 */
public interface ContextualIterator extends StructuredIterator {
  public DocumentContext getContext();
  public void setContext(DocumentContext context);
}
