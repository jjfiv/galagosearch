/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import java.io.IOException;

/**
 *
 * @author marc
 */
public interface ContextualIterator extends StructuredIterator {
  public DocumentContext getContext();
  public void setContext(DocumentContext context);
}
