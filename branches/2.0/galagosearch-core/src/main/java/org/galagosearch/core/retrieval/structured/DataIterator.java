/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.galagosearch.core.retrieval.structured;

import org.galagosearch.core.index.ValueIterator;

/**
 *
 * @author irmarc
 */
public interface DataIterator<T> extends StructuredIterator {
  public T getData();
}
