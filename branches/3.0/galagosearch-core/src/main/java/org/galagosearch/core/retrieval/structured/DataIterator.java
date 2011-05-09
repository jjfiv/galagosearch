package org.galagosearch.core.retrieval.structured;

/**
 *
 * @author irmarc
 */
public interface DataIterator<T> extends StructuredIterator {
  public T getData();
}
