package org.galagosearch.core.retrieval.structured;

/**
 * Currently represents the context that the entire query processor shares.
 *
 * @author irmarc
 */
public class DocumentContext {
  public int document;
  public int lastScored;
  public int length;
}
