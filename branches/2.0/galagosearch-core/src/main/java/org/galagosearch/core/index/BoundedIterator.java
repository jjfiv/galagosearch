/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.index;

import org.galagosearch.core.retrieval.structured.StructuredIterator;

/**
 *
 * @author irmarc
 */
public interface BoundedIterator extends StructuredIterator {
  public long totalEntries();
}
