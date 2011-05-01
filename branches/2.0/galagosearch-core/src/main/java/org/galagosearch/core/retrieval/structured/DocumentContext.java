package org.galagosearch.core.retrieval.structured;

/**
 * Currently represents the context that the entire query processor shares.
 * This is the most basic context we use
 *
 * @author irmarc
 */
public class DocumentContext {

  public DocumentContext() {}

  public DocumentContext(int d, int l) {
    document = d; length = l;
  }

  public int document;
  public int length;
}
